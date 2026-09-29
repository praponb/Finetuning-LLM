WITH joined AS (
    -- Attach stores attributes
    SELECT e.*, b.opened_date
    FROM store_sales e
    INNER JOIN stores b
        ON e.store_id = b.id
),
joined_store_sales AS (
    -- Attach stores attributes
    SELECT
        src.*,
        s.store_name,
        s.opened_date AS store_opened_date,
        s.city
    FROM joined src
    LEFT JOIN stores s
        ON src.store_id = s.id
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(revenue) OVER (PARTITION BY product_id) AS product_id_count_revenue,
        COUNT(revenue) OVER w_hist AS city_count_revenue
    FROM joined_store_sales
    WINDOW w_hist AS (PARTITION BY city, product_id)
),
summary AS (
    -- Multi-level aggregation
    SELECT
        store_id,
        ROUND(AVG(city_count_revenue), 2) AS avg_city_count_revenue,
        PERCENTILE_APPROX(units, ARRAY(0.25, 0.5, 0.75)) AS units_quartiles,
        SUM(CASE WHEN city = 'New York' THEN product_id_count_revenue ELSE 0 END) AS new_york_product_id_count_revenue
    FROM with_windows
    GROUP BY 1
)
SELECT
    store_id,
    avg_city_count_revenue,
    new_york_product_id_count_revenue
FROM summary;
