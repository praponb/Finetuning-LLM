WITH with_last AS (
    SELECT
        s.*,
        x.price,
        x.product_cost
    FROM suppliers s
    LEFT JOIN LATERAL (
            SELECT c.price, c.cost AS product_cost
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
latest AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY country, city ORDER BY rating DESC) AS row_num
        FROM with_last
    ) x
    WHERE row_num = 1
),
enriched AS (
    SELECT
        *,
        LEAD(rating, 2) OVER w_running AS next_rating,
        RANK() OVER (PARTITION BY country ORDER BY price DESC NULLS LAST) AS price_rank
    FROM latest
    WINDOW w_running AS (PARTITION BY country ORDER BY price, id)
),
latest_suppliers AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY price_rank NULLS LAST, id) AS row_num_2
    FROM enriched
    QUALIFY row_num_2 = 1
),
cleaned AS (
    SELECT
        *,
        CONCAT(supplier_name, ' - ', country) AS supplier_name_country,
        ROUND(rating / price, 2) AS rating_per_price,
        COALESCE(city, 'UNKNOWN') AS city_clean
    FROM latest_suppliers
    WHERE LENGTH(supplier_name) > 7
),
with_last_suppliers AS (
    SELECT
        s.id,
        s.rating,
        s.price,
        s.supplier_name_country,
        s.rating_per_price,
        s.city_clean,
        latest.product_cost_2
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.cost AS product_cost_2
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.launch_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
latest_match AS (
    SELECT
        s.id,
        s.rating,
        s.supplier_name_country,
        s.city_clean,
        s.product_cost_2,
        x.cost,
        x.brand
    FROM with_last_suppliers s
    LEFT JOIN LATERAL (
            SELECT c.cost, c.brand
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.price DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    brand,
    COUNT(*) FILTER (WHERE brand = 'Umbrella') AS umbrella_rows,
    COUNT(*) FILTER (WHERE brand = 'Umbrella') AS umbrella_rows_2,
    SUM(CASE WHEN brand = 'Umbrella' THEN product_cost_2 ELSE 0 END) AS umbrella_product_cost_2,
    ROUND(AVG(rating), 2) AS avg_rating,
    APPROX_PERCENTILE(cost, ARRAY(0.25, 0.5, 0.75)) AS cost_quartiles
FROM latest_match
GROUP BY 1
ORDER BY umbrella_rows_2
