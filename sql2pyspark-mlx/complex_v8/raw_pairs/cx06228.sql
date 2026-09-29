WITH valid AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at,
        CONCAT_WS(' / ', claim_type, status) AS claim_type_status_label,
        CASE WHEN status IN ('OPEN', 'REJECTED') THEN 'group_a' WHEN status = 'PAID' THEN 'group_b' ELSE 'other' END AS status_group
    FROM claims
),
exploded_detail AS (
    -- One row per claim
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.approved_amount,
        s.status_group,
        c.id AS claim_id,
        c.reported_at AS claim_reported_at,
        c.claimed_amount AS claim_claimed_amount
    FROM valid s
    JOIN claims c ON c.policy_id = s.policy_id
),
per_group AS (
    -- Group-level totals
    SELECT
        adjuster_id,
        PERCENTILE_APPROX(approved_amount, 0.5) AS median_approved_amount,
        SUM(claimed_amount) AS total_claimed_amount,
        MIN(claim_reported_at) AS first_claim_reported_at,
        COUNT(*) AS row_count,
        ROUND(STDDEV(approved_amount), 2) AS stddev_approved_amount
    FROM exploded_detail
    WHERE approved_amount >= 2569.2
    GROUP BY adjuster_id
    HAVING COUNT(*) >= 3
),
joined_adjuster AS (
    -- Enrich with dimension data
    SELECT
        s.adjuster_id,
        s.median_approved_amount,
        s.total_claimed_amount,
        s.first_claim_reported_at,
        a.adjuster_name
    FROM per_group s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        adjuster_id,
        total_claimed_amount,
        first_claim_reported_at,
        adjuster_name,
        LAG(median_approved_amount) OVER (PARTITION BY YEAR(first_claim_reported_at) ORDER BY total_claimed_amount, adjuster_id) AS prev_median_approved_amount,
        ROUND(AVG(median_approved_amount) OVER (PARTITION BY YEAR(first_claim_reported_at) ORDER BY total_claimed_amount, adjuster_id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS median_approved_amount_avg_6_prior,
        MIN(median_approved_amount) OVER (PARTITION BY TO_DATE(first_claim_reported_at), adjuster_id ORDER BY first_claim_reported_at ASC, adjuster_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_median_approved_amount_so_far,
        COUNT(*) OVER (PARTITION BY adjuster_id) AS rows_in_adjuster_id,
        ROUND(AVG(median_approved_amount) OVER (PARTITION BY adjuster_id), 2) AS adjuster_id_avg_median_approved_amount,
        NTH_VALUE(median_approved_amount, 2) OVER (PARTITION BY YEAR(first_claim_reported_at) ORDER BY total_claimed_amount, adjuster_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_median_approved_amount
    FROM joined_adjuster
)
-- Final projection
SELECT
    adjuster_id,
    total_claimed_amount,
    first_claim_reported_at,
    adjuster_name,
    prev_median_approved_amount,
    median_approved_amount_avg_6_prior,
    min_median_approved_amount_so_far,
    second_median_approved_amount,
    ROUND(rows_in_adjuster_id / NULLIF(total_claimed_amount, 0), 3) AS rows_in_adjuster_id_per_total_claimed_amount,
    DATE_FORMAT(first_claim_reported_at, 'yyyy-MM') AS first_claim_reported_at_label
FROM ranked
ORDER BY median_approved_amount_avg_6_prior
