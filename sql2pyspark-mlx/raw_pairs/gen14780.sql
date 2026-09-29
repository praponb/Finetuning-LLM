WITH joined AS (
    -- Attach suppliers attributes
    SELECT s.*, b.country
    FROM products s
    INNER JOIN suppliers b
        ON s.supplier_id = b.id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        product_name,
        price,
        launch_date,
        tags,
        country,
        AVG(stock_qty) OVER (PARTITION BY country, category ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_stock_qty,
        COUNT(stock_qty) OVER (PARTITION BY country, category ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_stock_qty,
        SUM(stock_qty) OVER (PARTITION BY country, category ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_stock_qty,
        FIRST_VALUE(country, TRUE) OVER (PARTITION BY brand, category ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_country
    FROM joined
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.product_id = s.id
              AND x.store_id IN (3, 5)
        )
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT_WS('-', product_name, country) AS product_name_country_key,
        CASE WHEN country = 'SG' THEN 1 ELSE 0 END AS is_sg,
        COALESCE(country, 'N/A') AS country_clean
    FROM eligible
    WHERE country IS NOT NULL
      AND running_sum_stock_qty < 600
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        running_count_stock_qty,
        running_sum_stock_qty,
        first_known_country,
        is_sg,
        country_clean,
        SUM(running_sum_stock_qty) OVER (PARTITION BY country) AS country_sum_running_sum_stock_qty,
        SUM(running_sum_stock_qty) OVER (ORDER BY running_sum_stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_running_sum_stock_qty,
        AVG(running_avg_stock_qty) OVER (PARTITION BY country ORDER BY is_sg, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS running_avg_stock_qty_avg_6_prior
    FROM filtered
    WHERE MONTH(launch_date) IN (3, 5, 12)
)
-- Window metrics over several different partitions
SELECT
    id,
    running_count_stock_qty,
    first_known_country,
    is_sg,
    country_clean,
    country_sum_running_sum_stock_qty,
    running_running_sum_stock_qty,
    running_avg_stock_qty_avg_6_prior,
    SUM(country_sum_running_sum_stock_qty) OVER (ORDER BY running_avg_stock_qty_avg_6_prior, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_country_sum_running_sum_stock_qty,
    AVG(country_sum_running_sum_stock_qty) OVER (ORDER BY running_avg_stock_qty_avg_6_prior, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_country_sum_running_sum_stock_qty,
    MAX(country_sum_running_sum_stock_qty) OVER (ORDER BY running_avg_stock_qty_avg_6_prior, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_country_sum_running_sum_stock_qty,
    MIN(country_sum_running_sum_stock_qty) OVER (ORDER BY running_avg_stock_qty_avg_6_prior, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_country_sum_running_sum_stock_qty
FROM scored;
