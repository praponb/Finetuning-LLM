WITH prepared_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        SUBSTRING(TRIM(claim_type), 1, 3) AS claim_type_prefix
    FROM claims
    WHERE reported_at >= TIMESTAMP '2025-01-25 13:00:00'
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(claim_date) IGNORE NULLS OVER (PARTITION BY claim_type ORDER BY claimed_amount ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_claim_date,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY MONTH(reported_at), claim_type ORDER BY claimed_amount DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS claimed_amount_avg_2,
        MAX(claimed_amount) OVER (PARTITION BY status) AS status_max_claimed_amount,
        ROUND(PERCENT_RANK() OVER (PARTITION BY MONTH(reported_at), claim_type ORDER BY claimed_amount DESC), 3) AS claimed_amount_percent_rank,
        MAX(claimed_amount) OVER (PARTITION BY policy_id) AS policy_id_max_claimed_amount,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER w_policy_id, 0), 4) AS claimed_amount_share_of_policy_id,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY status, TO_DATE(reported_at)), 2) AS claimed_amount_vs_status_reported_at_to_date_avg,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY status, TO_DATE(reported_at)), 2) AS status_reported_at_to_date_avg_claimed_amount,
        FIRST_VALUE(claimed_amount) IGNORE NULLS OVER (PARTITION BY MONTH(reported_at), claim_type ORDER BY claimed_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_claimed_amount,
        claimed_amount - LAG(claimed_amount) OVER w_all_by_claim_date AS claimed_amount_change,
        COUNT(*) OVER (PARTITION BY policy_id) AS rows_in_policy_id,
        FIRST_VALUE(claim_date) IGNORE NULLS OVER (PARTITION BY MONTH(reported_at), claim_type ORDER BY claimed_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_claim_date_2,
        SIZE(COLLECT_SET(adjuster_id) OVER w_status) AS distinct_adjuster_id_in_status,
        MIN(claimed_amount) OVER (PARTITION BY policy_id, status ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_claimed_amount_so_far
    FROM prepared_claims
    WINDOW w_policy_id AS (PARTITION BY policy_id), w_all_by_claim_date AS (ORDER BY claim_date ASC, id), w_status AS (PARTITION BY status)
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_FORMAT(first_known_claim_date, 'MMM yyyy') AS first_known_claim_date_label
    FROM ranked
    WHERE claimed_amount_vs_status_reported_at_to_date_avg <= 10000.0
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        adjuster_id,
        ROUND(AVG(claimed_amount), 2) AS avg_claimed_amount,
        ROUND(AVG(policy_id_max_claimed_amount), 2) AS avg_policy_id_max_claimed_amount,
        ROUND(STDDEV(claimed_amount), 2) AS stddev_claimed_amount
    FROM annotated
    WHERE YEAR(first_known_claim_date) = 2024
    GROUP BY 1
    HAVING AVG(status_reported_at_to_date_avg_claimed_amount) > 15070.0
)
-- Final projection
SELECT
    adjuster_id,
    avg_claimed_amount,
    stddev_claimed_amount,
    CASE WHEN stddev_claimed_amount >= 30000.0 THEN 'A' WHEN stddev_claimed_amount >= 20000.0 THEN 'B' ELSE 'C' END AS stddev_claimed_amount_tier,
    COALESCE(avg_claimed_amount, 0.0) AS avg_claimed_amount_filled
FROM summary
