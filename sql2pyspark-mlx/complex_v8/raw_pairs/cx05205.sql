WITH filtered AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        start_date,
        status,
        TRUNC(start_date, 'MM') AS start_date_month_start,
        CONCAT_WS('|', status, product) AS status_product_label,
        CONCAT(status, '#', CAST(holder_id AS STRING)) AS status_ref
    FROM policies
    WHERE premium BETWEEN 1132.1 AND 3465.4
      AND holder_id IS NOT NULL
),
qualified AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.status IN ('LAPSED', 'ACTIVE', 'CANCELLED')
        )
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT policy_id, COUNT(*) AS claim_count
    FROM claims
    GROUP BY policy_id
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_count, 0) AS claim_count
    FROM qualified s
    LEFT JOIN claim_stats c ON c.policy_id = s.id
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    start_date,
    status,
    start_date_month_start,
    status_product_label,
    status_ref,
    claim_count
FROM with_stats
WHERE status_product_label IS NOT NULL
