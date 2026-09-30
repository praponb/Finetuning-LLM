WITH valid_suppliers AS (
    SELECT *
    FROM suppliers
    WHERE country NOT IN ('JP', 'DE', 'UK') AND LOWER(TRIM(supplier_name)) = 'global supply'
),
agg AS (
    SELECT
        CASE WHEN rating >= 3.6 THEN 'high' WHEN rating >= 2.0 THEN 'mid' ELSE 'low' END AS rating_bucket,
        country,
        COUNT(*) AS cnt,
        MIN(rating) AS min_rating
    FROM valid_suppliers
    GROUP BY CASE WHEN rating >= 3.6 THEN 'high' WHEN rating >= 2.0 THEN 'mid' ELSE 'low' END, country
)
SELECT
    rating_bucket,
    country,
    cnt,
    min_rating
FROM agg
