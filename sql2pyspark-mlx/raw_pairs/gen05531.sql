WITH filtered AS (
    SELECT
        *,
        ARRAY_JOIN(tags, ', ') AS tags_text,
        COALESCE(brand, 'UNKNOWN') AS resolved_brand
    FROM products
    WHERE brand = 'Acme'
),
metrics AS (
    SELECT
        *,
        CORR(cost, price) OVER (PARTITION BY brand) AS corr_cost_price,
        DENSE_RANK() OVER (PARTITION BY category ORDER BY price) AS price_dense_rank,
        AVG(price) OVER (PARTITION BY category ORDER BY stock_qty, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS price_avg_2_prior
    FROM filtered
),
kept AS (
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.product_id = s.id
              AND ch.last_restock <= '2023-09-26'
        )
),
eligible AS (
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.product_id = s.id
              AND x.revenue <= 1900
        )
),
joined AS (
    SELECT
        e.id,
        e.product_name,
        e.stock_qty,
        e.supplier_id,
        e.price_avg_2_prior,
        d.country,
        d.rating,
        d.city
    FROM eligible e
    LEFT JOIN suppliers d
        ON e.supplier_id <=> d.id
),
base AS (
    SELECT
        id,
        product_name,
        stock_qty,
        price_avg_2_prior,
        country,
        rating,
        city,
        CONCAT_WS('-', city, product_name) AS city_product_name_key,
        CASE WHEN price_avg_2_prior >= 937 THEN 'High' WHEN price_avg_2_prior >= 363 THEN 'Medium' ELSE 'Low' END AS price_avg_2_prior_level
    FROM joined
    WHERE supplier_id IS NOT NULL
      AND rating BETWEEN 2.3 AND 2.7
),
enriched AS (
    SELECT
        id,
        product_name,
        country,
        rating,
        price_avg_2_prior_level,
        AVG(rating) OVER (PARTITION BY country ORDER BY price_avg_2_prior, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS rating_avg_5,
        CUME_DIST() OVER (PARTITION BY city ORDER BY stock_qty DESC NULLS FIRST) AS stock_qty_cume_dist,
        AVG(stock_qty) OVER (PARTITION BY city ORDER BY stock_qty, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS stock_qty_avg_6
    FROM base
)
SELECT
    country,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list,
    MIN(rating_avg_5) AS min_rating_avg_5,
    SIZE(COLLECT_SET(country)) AS num_countries,
    ROUND(AVG(rating_avg_5), 2) AS avg_rating_avg_5
FROM enriched
GROUP BY country
ORDER BY country NULLS FIRST
