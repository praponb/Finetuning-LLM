WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(quantity * 0.85, 2) AS quantity_adjusted
    FROM inventory
    WHERE store_id = 2
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY last_restock DESC NULLS FIRST, id) AS latest_rank
    FROM base
    QUALIFY latest_rank = 1
),
joined AS (
    -- Attach stores attributes
    SELECT
        s.id,
        s.store_id,
        s.product_id,
        s.quantity,
        s.last_restock,
        p2.region,
        p2.city
    FROM latest s
    LEFT JOIN stores p2
        ON s.store_id = p2.id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        product_id,
        last_restock,
        city,
        AVG(quantity) OVER w_part AS quantity_avg_2_prior,
        SUM(quantity) OVER (PARTITION BY region ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity,
        FIRST_VALUE(region) IGNORE NULLS OVER (PARTITION BY city ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_region
    FROM joined
    WHERE product_id = 3
    WINDOW w_part AS (PARTITION BY YEAR(last_restock), city ORDER BY quantity, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)
)
-- Aggregate with conditional metrics
SELECT
    city,
    product_id,
    store_id,
    SUM(CASE WHEN city = 'Berlin' THEN cumulative_quantity ELSE 0 END) AS berlin_cumulative_quantity,
    SIZE(COLLECT_SET(city)) AS num_cities
FROM scored
GROUP BY city, product_id, store_id;
