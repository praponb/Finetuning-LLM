WITH with_dims AS (
    SELECT
        a.*,
        d.supplier_name,
        d.city,
        d.country
    FROM products a
    LEFT JOIN suppliers d
        ON a.supplier_id = d.id
),
enriched AS (
    SELECT
        a.*,
        s.rating,
        s.country AS supplier_country,
        s.city AS supplier_city
    FROM with_dims a
    INNER JOIN suppliers s
        ON a.supplier_id = s.id
),
cleaned AS (
    SELECT
        id,
        category,
        brand,
        cost,
        launch_date,
        supplier_id,
        supplier_name,
        rating,
        supplier_country,
        supplier_city,
        ROUND(price / rating, 2) AS price_per_rating,
        COALESCE(country, 'DIRECT') AS country_filled
    FROM enriched
    WHERE LOWER(TRIM(supplier_name)) = 'global supply'
)
SELECT
    supplier_id,
    DATE_TRUNC('MONTH', launch_date) AS activity_month,
    SORT_ARRAY(COLLECT_SET(supplier_country)) AS supplier_country_list,
    COUNT(*) FILTER (WHERE brand = 'Stark') AS stark_rows,
    AVG(price_per_rating) FILTER (WHERE supplier_city = 'Berlin') AS berlin_avg_price_per_rating
FROM cleaned
GROUP BY GROUPING SETS ((supplier_id, DATE_TRUNC('MONTH', launch_date)), (supplier_id), ())
HAVING COUNT(*) FILTER (WHERE brand = 'Stark') >= 1
ORDER BY stark_rows DESC NULLS LAST;
