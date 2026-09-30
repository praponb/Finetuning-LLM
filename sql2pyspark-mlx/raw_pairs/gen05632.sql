WITH cleaned AS (
    SELECT *, LENGTH(country) AS country_length
    FROM suppliers
    WHERE city IS NOT NULL
),
deduped AS (
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC NULLS FIRST, id) = 1
),
deduped_suppliers AS (
    SELECT
        id,
        supplier_name,
        city,
        rating
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating NULLS FIRST, id) AS latest_rank
        FROM deduped
    ) t
    WHERE latest_rank = 1
),
latest_match AS (
    SELECT s.*, top1.product_cost
    FROM deduped_suppliers s
    LEFT JOIN LATERAL (
            SELECT c.cost AS product_cost
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.launch_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    city,
    CASE WHEN rating >= 4.0 THEN 'PREMIUM' ELSE 'STANDARD' END AS rating_tier,
    AVG(rating) AS avg_rating,
    STDDEV_SAMP(rating) AS rating_stddev,
    COUNT_IF(city = 'Bangkok') AS bangkok_events
FROM latest_match
GROUP BY GROUPING SETS ((city, CASE WHEN rating >= 4.0 THEN 'PREMIUM' ELSE 'STANDARD' END), (city))
ORDER BY city;
