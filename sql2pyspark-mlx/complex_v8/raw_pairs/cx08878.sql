WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        CASE WHEN claimed_amount >= 29985.9 THEN 'large' WHEN claimed_amount >= 20436.5 THEN 'medium' ELSE 'small' END AS claimed_amount_tier
    FROM claims
    WHERE approved_amount <= 25402.3
      AND HOUR(reported_at) BETWEEN 9 AND 14
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY status) AS rows_in_status,
        ROUND(CUME_DIST() OVER (PARTITION BY claimed_amount_tier, policy_id ORDER BY claim_date), 3) AS claim_date_cume_dist
    FROM recent
),
qualified AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM windowed s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.claimed_amount BETWEEN 6745.1 AND 42447.2
        )
      AND s.adjuster_id IS NOT NULL
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY claimed_amount_tier, MONTH(claim_date))) AS distinct_status_in_claimed_amount_tier_claim_dat,
        COUNT(*) OVER w_claimed_amount_tier_claim_da AS rows_in_claimed_amount_tier_claim_date_month,
        ROUND(PERCENT_RANK() OVER (ORDER BY approved_amount DESC), 3) AS approved_amount_percent_rank,
        SUM(claim_date_cume_dist) OVER (PARTITION BY claimed_amount_tier, MONTH(claim_date)) AS claimed_amount_tier_claim_date_month_sum_claim_d,
        LEAD(claimed_amount) OVER (ORDER BY claim_date_cume_dist DESC, id) AS next_claimed_amount
    FROM qualified
    WINDOW w_claimed_amount_tier_claim_da AS (PARTITION BY claimed_amount_tier, MONTH(claim_date))
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric, metric_value
FROM (
    SELECT id, rows_in_status, distinct_status_in_claimed_amount_tier_claim_dat
    FROM ranked
) src
UNPIVOT (
    metric_value FOR metric IN (rows_in_status AS rows_in_status_v, distinct_status_in_claimed_amount_tier_claim_dat AS m_distinct_status_in_claimed_amount_tier_claim_d)
) unp
ORDER BY id, metric
