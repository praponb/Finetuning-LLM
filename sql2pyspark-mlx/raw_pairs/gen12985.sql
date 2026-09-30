WITH base AS (
    SELECT *, CAST(quantity AS DOUBLE) AS quantity_str
    FROM inventory
    WHERE quantity >= 63
),
enriched AS (
    SELECT
        src.id,
        src.product_id,
        src.quantity,
        b.brand,
        b.category,
        b.price
    FROM base src
    LEFT JOIN products b
        ON src.product_id = b.id
    WHERE b.supplier_id IN (18, 37, 39)
),
enriched_inventory AS (
    SELECT
        id,
        category,
        price,
        STDDEV_SAMP(price) OVER (PARTITION BY brand) AS brand_stddev_samp_price,
        AVG(quantity) OVER (PARTITION BY category, brand ORDER BY price, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS quantity_avg_5_prior,
        SUM(quantity) OVER (PARTITION BY product_id, brand ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity,
        AVG(quantity) OVER (PARTITION BY category, product_id ORDER BY quantity, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS quantity_avg_6,
        DENSE_RANK() OVER (ORDER BY price NULLS LAST) AS price_dense_rank
    FROM enriched
),
ranked AS (
    SELECT *
    FROM enriched_inventory
    QUALIFY ROW_NUMBER() OVER (PARTITION BY category ORDER BY cumulative_quantity DESC NULLS FIRST, id) = 1
),
enriched_inventory_2 AS (
    SELECT
        *,
        SUM(cumulative_quantity) OVER (ORDER BY cumulative_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cumulative_quantity,
        MIN(brand_stddev_samp_price) OVER (PARTITION BY category) AS category_min_brand_stddev_samp_price
    FROM ranked
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY cumulative_cumulative_quantity NULLS LAST, id) AS rank_in_group
    FROM enriched_inventory_2
    QUALIFY rank_in_group = 1
)
SELECT
    CASE WHEN cumulative_cumulative_quantity >= 4629 THEN 'LARGE' ELSE 'SMALL' END AS cumulative_cumulative_quantity_tier,
    CASE WHEN brand_stddev_samp_price >= 953 THEN 'HIGH' ELSE 'LOW' END AS brand_stddev_samp_price_tier,
    SUM(CASE WHEN category = 'Sports' THEN price ELSE 0 END) AS sports_price,
    ROUND(AVG(brand_stddev_samp_price), 2) AS avg_brand_stddev_samp_price,
    ROUND(SUM(price) / NULLIF(COUNT(CASE WHEN category = 'Books' THEN 1 END), 0), 2) AS price_per_books,
    PERCENTILE_APPROX(cumulative_quantity, 0.95) AS cumulative_quantity_p95
FROM deduped
GROUP BY ROLLUP (CASE WHEN cumulative_cumulative_quantity >= 4629 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN brand_stddev_samp_price >= 953 THEN 'HIGH' ELSE 'LOW' END)
ORDER BY cumulative_cumulative_quantity_tier;
