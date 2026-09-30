WITH cleaned_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        claimed_amount - approved_amount AS claimed_amount_minus_approved_amount
    FROM claims
    WHERE status IN ('REJECTED', 'OPEN', 'APPROVED')
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY policy_id ORDER BY approved_amount ASC, id) AS approved_amount_seq,
        SUM(claimed_amount) OVER (PARTITION BY policy_id ORDER BY approved_amount ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS claimed_amount_sum_3,
        ROW_NUMBER() OVER (PARTITION BY policy_id, adjuster_id ORDER BY claim_date NULLS LAST, id) AS rn,
        COUNT(*) OVER w_adjuster_id AS rows_in_adjuster_id,
        NTH_VALUE(claim_date, 2) OVER (PARTITION BY policy_id, adjuster_id ORDER BY claim_date NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claim_date,
        NTH_VALUE(claimed_amount, 2) OVER (PARTITION BY policy_id, adjuster_id ORDER BY claim_date NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claimed_amount,
        ROW_NUMBER() OVER (PARTITION BY adjuster_id, policy_id ORDER BY approved_amount, id) AS seq_no,
        DATEDIFF(claim_date, LAG(claim_date) OVER (PARTITION BY policy_id, adjuster_id ORDER BY claim_date NULLS LAST, id)) AS days_since_prev_claim_date,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY policy_id ORDER BY approved_amount ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS claimed_amount_avg_3
    FROM cleaned_claims
    WINDOW w_adjuster_id AS (PARTITION BY adjuster_id)
),
top_rows AS (
    -- Top 5 per group
    SELECT *
    FROM windowed
    WHERE rn <= 5
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(claim_date) IGNORE NULLS OVER (PARTITION BY YEAR(claim_date), approved_amount_seq ORDER BY approved_amount_seq DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_claim_date,
        ROUND(AVG(approved_amount_seq) OVER (PARTITION BY rn, adjuster_id), 2) AS rn_adjuster_id_avg_approved_amount_seq,
        SUM(approved_amount) OVER w_rn_adjuster_id AS rn_adjuster_id_sum_approved_amount,
        ROUND(CUME_DIST() OVER (PARTITION BY YEAR(claim_date) ORDER BY days_since_prev_claim_date DESC), 3) AS days_since_prev_claim_date_cume_dist,
        ROUND(days_since_prev_claim_date - AVG(days_since_prev_claim_date) OVER (PARTITION BY rn, adjuster_id), 2) AS days_since_prev_claim_date_vs_rn_adjuster_id_avg,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER (PARTITION BY rn, adjuster_id), 0), 4) AS claimed_amount_share_of_rn_adjuster_id,
        MAX(seq_no) OVER (PARTITION BY adjuster_id ORDER BY claimed_amount_avg_3, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS seq_no_max_4
    FROM top_rows
    WINDOW w_rn_adjuster_id AS (PARTITION BY rn, adjuster_id)
),
per_group AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', first_known_claim_date) AS first_known_claim_date_month,
        ROUND(AVG(days_since_prev_claim_date_vs_rn_adjuster_id_avg), 2) AS avg_days_since_prev_claim_date_vs_rn_adjuster_id,
        ROUND(SUM(approved_amount) / NULLIF(SUM(claimed_amount_share_of_rn_adjuster_id), 0), 3) AS approved_amount_to_claimed_amount_share_of_rn_ad,
        COUNT(DISTINCT adjuster_id) AS distinct_adjuster_ids
    FROM metrics
    GROUP BY DATE_TRUNC('MONTH', first_known_claim_date)
    HAVING AVG(claimed_amount_avg_3) > 15070.0
)
SELECT
    first_known_claim_date_month,
    distinct_adjuster_ids,
    CASE WHEN distinct_adjuster_ids >= 6 THEN 'A' WHEN distinct_adjuster_ids >= 3 THEN 'B' ELSE 'C' END AS distinct_adjuster_ids_tier,
    FLOOR(distinct_adjuster_ids / 2) * 2 AS distinct_adjuster_ids_bucket
FROM per_group
WHERE distinct_adjuster_ids >= 4
ORDER BY distinct_adjuster_ids_tier DESC
