WITH recent_suppliers AS (
    SELECT *, SUBSTRING(country, 1, 4) country_prefix
    FROM suppliers
    WHERE country IN ('DE', 'UK', 'TH')
),
agg AS (
    SELECT CASE WHEN rating >= 4.1 THEN 'high' WHEN rating >= 1.4 THEN 'mid' ELSE 'low' END rating_bucket, MIN(rating) min_rating
    FROM recent_suppliers
    GROUP BY CASE WHEN rating >= 4.1 THEN 'high' WHEN rating >= 1.4 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM agg
WHERE min_rating < 4.0
