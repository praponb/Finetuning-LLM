WITH base AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
)
SELECT holder_id, status AS name
FROM base
WHERE holder_id IS NOT NULL
  AND product = 'HEALTH'
INTERSECT
SELECT holder_id, product
FROM policies
ORDER BY holder_id DESC
