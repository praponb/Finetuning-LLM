WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), launch_date) days_since_launch_date,
        CONCAT_WS('/', product_name, brand) product_name_brand_key
    FROM products
    WHERE launch_date >= DATE '2023-12-30'
      AND supplier_id IS NOT NULL
      AND price BETWEEN 370 AND 1080
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.product_id = s.id
              AND c.order_id = 40
        )
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(days_since_launch_date) OVER (PARTITION BY supplier_id ORDER BY cost, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) days_since_launch_date_avg_2,
        FIRST_VALUE(stock_qty, TRUE) OVER (ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_stock_qty,
        LAST_VALUE(stock_qty) IGNORE NULLS OVER (PARTITION BY category ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_stock_qty,
        CUME_DIST() OVER (PARTITION BY supplier_id ORDER BY price DESC NULLS LAST) price_cume_dist
    FROM eligible
),
joined AS (
    -- Attach suppliers attributes
    SELECT
        src.*,
        p.country,
        p.city
    FROM enriched src
    LEFT JOIN suppliers p
        ON src.supplier_id = p.id
),
enriched_products AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(days_since_launch_date) OVER (PARTITION BY city ORDER BY price_cume_dist, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) days_since_launch_date_avg_6,
        LEAD(cost, 1) OVER (ORDER BY stock_qty, id) next_cost,
        PERCENT_RANK() OVER (PARTITION BY city, category ORDER BY last_known_stock_qty NULLS FIRST) last_known_stock_qty_percent_rank
    FROM joined
),
enriched_products_2 AS (
    -- Attach suppliers attributes
    SELECT
        e.*,
        d.city AS supplier_city,
        d.supplier_name,
        d.rating
    FROM enriched_products e
    LEFT JOIN suppliers d
        ON e.supplier_id = d.id
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_name,
        brand,
        price,
        stock_qty,
        launch_date,
        supplier_id,
        days_since_launch_date,
        days_since_launch_date_avg_2,
        first_known_stock_qty,
        last_known_stock_qty,
        country,
        next_cost,
        supplier_city,
        supplier_name,
        rating,
        SUBSTRING(city, 1, 3) city_prefix,
        COALESCE(city, 'OTHER') city_clean
    FROM enriched_products_2
    WHERE launch_date > DATE '2023-09-01'
      AND brand IS NOT NULL
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        supplier_city,
        next_cost,
        rating,
        price
    FROM filtered
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR kpi IN (next_cost AS NEXT_COST_VAL, rating, price)
);
