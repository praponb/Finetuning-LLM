WITH valid AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status
    FROM policies
    WHERE start_date < DATE '2025-03-21'
      AND status <> 'LAPSED'
),
with_details_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.status,
        p.holder_name
    FROM valid s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE LENGTH(TRIM(p.holder_name)) >= 8
),
with_activity AS (
    -- Keep rows whose holder matches
    SELECT s.*
    FROM with_details_holder s
    WHERE EXISTS (
            SELECT 1
            FROM policyholders p
            WHERE p.id = s.holder_id
              AND UPPER(TRIM(p.holder_name)) LIKE 'G%'
              AND p.risk_score > 0.1
        )
)
-- Subtotals with GROUPING SETS
SELECT
    holder_id,
    COALESCE(status, 'ALL') AS status,
    GROUPING(holder_id) AS is_total_holder_id,
    COUNT(*) AS n_rows
FROM with_activity
GROUP BY GROUPING SETS ((holder_id, status), (holder_id))
ORDER BY holder_id NULLS LAST, status NULLS LAST
