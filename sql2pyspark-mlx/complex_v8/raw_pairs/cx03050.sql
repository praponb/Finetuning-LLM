WITH recent_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        start_date,
        end_date,
        status,
        CONCAT_WS(': ', status, product) AS status_product_label
    FROM policies
    WHERE start_date < DATE '2025-02-14'
),
with_activity AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM recent_policys s
    WHERE EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.end_date < DATE '2026-08-31'
        )
),
without_matches AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM with_activity s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND YEAR(c.claim_date) = 2024
        )
      AND s.holder_id >= 10
)
SELECT
    status,
    holder_id,
    GROUPING_ID() AS grouping_level,
    COUNT(DISTINCT holder_id) AS distinct_holder_ids,
    SUM(CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END) AS n_status_active
FROM without_matches
GROUP BY GROUPING SETS ((status, holder_id), (status), (holder_id))
ORDER BY status NULLS LAST, holder_id NULLS LAST
