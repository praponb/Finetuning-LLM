WITH remaining_keys AS (
    SELECT city
    FROM suppliers
    INTERSECT
    SELECT city
    FROM suppliers
    WHERE country != 'JP'
),
verified AS (
    SELECT s.*
    FROM suppliers s
    JOIN remaining_keys k ON s.city = k.city
),
metrics AS (
    SELECT
        *,
        ROUND(rating / SUM(rating) OVER (PARTITION BY city), 4) AS rating_share,
        ROUND(rating / SUM(rating) OVER (PARTITION BY city), 4) AS rating_share_2
    FROM verified
),
kept AS (
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.stock_qty = 365
        )
),
eligible AS (
    SELECT s.*
    FROM kept s
    WHERE EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.cost >= 480
        )
),
valid AS (
    SELECT
        *,
        rating_share_2 * rating AS rating_share_2_times_rating,
        CONCAT(city, ', ', supplier_name) AS city_supplier_name,
        COALESCE(city, 'OTHER') AS city_clean
    FROM eligible
    WHERE city IS NOT NULL
      AND city IS DISTINCT FROM 'Tokyo'
)
SELECT *
FROM (
    SELECT
        city,
        country,
        rating_share_2
    FROM valid
) src
PIVOT (
    AVG(rating_share_2) AS value_sum
    FOR country IN ('US' AS us, 'JP' AS jp)
);
