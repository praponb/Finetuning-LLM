WITH filtered AS (
    SELECT *, CONCAT(supplier_name, ', ', country) supplier_name_country
    FROM suppliers
    WHERE city IS NOT NULL
),
scored AS (
    SELECT
        *,
        MIN(rating) OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_min_rating,
        MAX(rating) OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_max_rating,
        AVG(rating) OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_avg_rating,
        SUM(rating) OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_sum_rating
    FROM filtered
),
matched_keys AS (
    SELECT id
    FROM scored
    EXCEPT DISTINCT
    SELECT id
    FROM suppliers
    WHERE LOWER(supplier_name) LIKE '%ion%'
),
kept_rows AS (
    SELECT s.*
    FROM scored s
    INNER JOIN matched_keys k ON s.id = k.id
),
valid AS (
    SELECT
        *,
        ROUND(last3_avg_rating, 1) last3_avg_rating_rounded,
        UPPER(city) city_upper,
        COALESCE(city, 'NONE') city_filled
    FROM kept_rows
    WHERE city IS NOT NULL
)
SELECT *
FROM (
    SELECT
        country,
        city,
        last3_sum_rating
    FROM valid
) src
PIVOT (
    SUM(last3_sum_rating) AS amount,
    COUNT(1) AS item_count
    FOR city IN ('Singapore' AS singapore, 'New York' AS new_york)
)
