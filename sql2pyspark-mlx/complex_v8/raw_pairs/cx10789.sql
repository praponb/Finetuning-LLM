WITH prepared AS (
    -- Scope the warehouses rows
    SELECT
        id,
        wh_code,
        city,
        opened_on
    FROM warehouses
    WHERE city = 'Singapore'
      AND opened_on < DATE '2025-07-03'
),
deduped AS (
    -- Keep the latest row per city
    SELECT
        id,
        wh_code,
        city,
        opened_on
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_on DESC, id DESC) AS row_num
        FROM prepared
    ) ranked
    WHERE row_num = 1
)
-- Final projection
SELECT
    id,
    wh_code,
    city,
    opened_on,
    DATE_FORMAT(opened_on, 'yyyy-MM') AS opened_on_label
FROM deduped
ORDER BY id
