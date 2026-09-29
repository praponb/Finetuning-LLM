WITH cleaned AS (
    -- Filter adjusters
    SELECT
        id,
        adjuster_name,
        team,
        level
    FROM adjusters
),
detail_rows AS (
    -- One row per claim
    SELECT
        s.id,
        s.adjuster_name,
        s.team,
        s.level,
        c.id AS claim_id,
        c.status AS claim_status,
        c.approved_amount AS claim_approved_amount
    FROM cleaned s
    JOIN claims c ON c.adjuster_id = s.id AND c.status = 'OPEN'
),
joined AS (
    -- Join claims
    SELECT
        s.id,
        s.team,
        s.claim_id,
        s.claim_status,
        s.claim_approved_amount,
        c.claim_date,
        c.policy_id AS claim_policy_id,
        c.approved_amount AS claim_approved_amount_2,
        p.product AS policy_product
    FROM detail_rows s
    JOIN claims c ON c.id = s.claim_id
    LEFT JOIN policies p ON p.id = c.policy_id
),
grouped AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', claim_date) AS claim_date_month,
        SUM(claim_approved_amount) AS total_claim_approved_amount,
        COUNT(DISTINCT claim_status) AS distinct_claim_status,
        ROUND(STDDEV(claim_approved_amount_2), 2) AS stddev_claim_approved_amount_2
    FROM joined
    WHERE team <> 'Motor'
    GROUP BY 1
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM grouped
    WHERE total_claim_approved_amount > (SELECT AVG(total_claim_approved_amount) FROM grouped)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY distinct_claim_status ORDER BY distinct_claim_status DESC NULLS FIRST), 3) AS distinct_claim_status_cume_dist,
        ROUND(AVG(total_claim_approved_amount) OVER (PARTITION BY distinct_claim_status ORDER BY distinct_claim_status DESC NULLS FIRST, claim_date_month ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS total_claim_approved_amount_avg_1_prior,
        ROUND(total_claim_approved_amount - AVG(total_claim_approved_amount) OVER w_distinct_claim_status_claim, 2) AS total_claim_approved_amount_vs_distinct_claim_st,
        ROUND(distinct_claim_status / NULLIF(SUM(distinct_claim_status) OVER w_distinct_claim_status_claim, 0), 4) AS distinct_claim_status_share_of_distinct_claim_st,
        SUM(distinct_claim_status) OVER w_distinct_claim_status_claim AS distinct_claim_status_claim_date_month_month_sum,
        SUM(distinct_claim_status) OVER (ORDER BY stddev_claim_approved_amount_2 DESC, claim_date_month ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS distinct_claim_status_sum_6_prior
    FROM filtered
    WINDOW w_distinct_claim_status_claim AS (PARTITION BY distinct_claim_status, MONTH(claim_date_month))
)
-- Final projection
SELECT
    claim_date_month,
    total_claim_approved_amount,
    distinct_claim_status,
    stddev_claim_approved_amount_2,
    total_claim_approved_amount_vs_distinct_claim_st,
    distinct_claim_status_share_of_distinct_claim_st,
    distinct_claim_status_claim_date_month_month_sum,
    distinct_claim_status_sum_6_prior
FROM metrics
ORDER BY distinct_claim_status_sum_6_prior DESC NULLS FIRST, stddev_claim_approved_amount_2
