WITH filtered_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        LOWER(TRIM(status)) AS status_clean,
        CASE WHEN status IN ('CANCELLED', 'EXPIRED') THEN 'group_a' WHEN status = 'LAPSED' THEN 'group_b' ELSE 'other' END AS status_group,
        LOWER(TRIM(status)) AS status_clean_2
    FROM policies
    WHERE product IS NOT NULL
      AND holder_id >= 10
),
with_activity AS (
    -- Keep rows whose holder matches
    SELECT s.*
    FROM filtered_policys s
    WHERE EXISTS (
            SELECT 1
            FROM policyholders p
            WHERE p.id = s.holder_id
              AND p.joined_on IS NOT NULL
              AND p.risk_score < 0.8
        )
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MIN(c.reported_at) FROM claims c WHERE c.policy_id = s.id) AS first_claim_reported_at,
        (SELECT COUNT(CASE WHEN c.claim_type IN ('fire', 'delay') THEN 1 END) FROM claims c WHERE c.policy_id = s.id) AS claim_claim_type_count
    FROM with_activity s
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER (ORDER BY start_date DESC), 3) AS start_date_cume_dist,
        ROW_NUMBER() OVER (ORDER BY start_date DESC, id) AS seq_no,
        LAST_VALUE(first_claim_reported_at) IGNORE NULLS OVER (ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_first_claim_reported_at,
        COUNT(*) OVER w_product_end_date_month AS rows_in_product_end_date_month
    FROM profiled
    WINDOW w_product_end_date_month AS (PARTITION BY product, MONTH(end_date))
)
-- Subtotals with GROUPING SETS
SELECT
    status_group,
    seq_no,
    GROUPING_ID() AS grouping_level,
    COUNT(DISTINCT id) AS distinct_ids,
    ARRAY_SORT(COLLECT_SET(product)) AS product_list
FROM windowed
GROUP BY GROUPING SETS ((status_group, seq_no), (status_group))
