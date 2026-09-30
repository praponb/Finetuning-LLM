WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, UPPER(TRIM(supplier_name)) AS supplier_name_normalized
    FROM suppliers
    WHERE supplier_name NOT LIKE 'L%'
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY rating, id) AS row_num
    FROM cleaned
    QUALIFY row_num = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        supplier_name_normalized,
        row_num,
        LEAD(rating, 2) OVER (PARTITION BY country, city ORDER BY row_num, id) AS next_rating,
        LAST_VALUE(city) IGNORE NULLS OVER (PARTITION BY city, country ORDER BY row_num, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_city,
        PERCENT_RANK() OVER (PARTITION BY city ORDER BY rating) AS rating_percent_rank
    FROM ranked
    WHERE row_num <= 1
),
ranked_suppliers AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY country ORDER BY next_rating NULLS LAST) AS rank_in_group
        FROM scored
    ) t
    WHERE rank_in_group = 1
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY rank_in_group DESC, id) AS row_num_2
    FROM ranked_suppliers
    QUALIFY row_num_2 = 1
),
latest_match AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.launch_date,
        x.stock_qty
    FROM deduped s
    LEFT JOIN LATERAL (
            SELECT c.launch_date, c.stock_qty
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.launch_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
-- Grouped metrics
SELECT
    country,
    DATE_TRUNC('YEAR', launch_date) AS year_start,
    SIZE(COLLECT_SET(city)) AS num_cities,
    SUM(CASE WHEN city = 'Berlin' THEN rating_percent_rank ELSE 0 END) AS berlin_rating_percent_rank,
    ROUND(AVG(next_rating), 2) AS avg_next_rating,
    APPROX_PERCENTILE(next_rating, 0.95) AS next_rating_p95,
    RANK() OVER (PARTITION BY country ORDER BY APPROX_PERCENTILE(next_rating, 0.95) DESC) AS next_rating_p95_rank
FROM latest_match
GROUP BY 1, 2
ORDER BY next_rating_p95 DESC NULLS LAST
