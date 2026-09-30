WITH cleaned AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        start_date,
        end_date,
        status,
        ROUND(premium * 1.1, 2) AS premium_adj,
        CONCAT_WS('-', status, product) AS status_product_label,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug
    FROM policies
    WHERE (premium < 3719.7 OR start_date < DATE '2025-02-14')
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.status,
        s.premium_adj,
        s.status_product_label,
        p.segment AS holder_segment
    FROM cleaned s
    JOIN policyholders p ON p.id = s.holder_id
),
screened AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM joined s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
        )
      AND s.holder_id >= 10
)
-- Final projection
SELECT
    id,
    holder_id,
    start_date,
    status,
    premium_adj,
    status_product_label,
    holder_segment
FROM screened
WHERE start_date < DATE '2025-05-02'
ORDER BY start_date NULLS LAST, status_product_label DESC, id
LIMIT 5
