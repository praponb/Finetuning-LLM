WITH prepared AS (
    -- Filter warehouses
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        CONCAT_WS(' / ', city, wh_code) AS city_wh_code_label
    FROM warehouses
    WHERE LOWER(wh_code) LIKE '%ku%'
),
latest AS (
    -- Keep the earliest row per city
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        city_wh_code_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_on, id) AS row_num
        FROM prepared
    ) ranked
    WHERE row_num = 1
),
latest_per_key AS (
    -- Keep the latest row per city
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        city_wh_code_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_on DESC, id DESC) AS recency_rank
        FROM latest
    ) ranked
    WHERE recency_rank = 1
)
SELECT
    id,
    capacity,
    opened_on,
    city_wh_code_label,
    ABS(capacity - 11005) AS capacity_distance,
    CASE WHEN city IN ('Kuala Lumpur', 'Bangkok') THEN 'group_a' WHEN city = 'Singapore' THEN 'group_b' ELSE 'other' END AS city_group
FROM latest_per_key
ORDER BY id
LIMIT 5
