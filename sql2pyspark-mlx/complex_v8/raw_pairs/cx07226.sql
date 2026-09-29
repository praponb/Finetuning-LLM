WITH cleaned AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        CONCAT(product, '#', CAST(id AS STRING)) AS product_ref,
        DATE_TRUNC('QUARTER', end_date) AS end_date_quarter
    FROM policies
    WHERE holder_id IS NOT NULL
),
qualified AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.status NOT IN ('EXPIRED', 'LAPSED')
        )
      AND s.end_date >= DATE '2026-01-23'
),
qualified_2 AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM qualified s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.status NOT IN ('PAID', 'REJECTED')
              AND c.approved_amount < 19337.6
        )
),
with_activity AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM qualified_2 s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.claim_type IN ('flood', 'delay')
        )
),
screened AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM with_activity s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.premium >= 2013.8
        )
      AND s.premium >= 1051.9
)
SELECT
    holder_id,
    COALESCE(product, 'ALL') AS product,
    COALESCE(status, 'ALL') AS status,
    SUM(premium) AS total_premium
FROM screened
GROUP BY ROLLUP(holder_id, product, status)
ORDER BY holder_id NULLS LAST, product NULLS LAST, status NULLS LAST
