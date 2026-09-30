WITH ValidProducts AS (
    SELECT *
    FROM products
    WHERE ARRAY_CONTAINS(tags, 'clearance')
),
cost_bucket_summary AS (
    SELECT CASE WHEN cost >= 640 THEN 'high' WHEN cost >= 320 THEN 'mid' ELSE 'low' END AS cost_bucket, COUNT(CASE WHEN brand = 'Acme' THEN 1 END) AS acme_count
    FROM ValidProducts
    GROUP BY CASE WHEN cost >= 640 THEN 'high' WHEN cost >= 320 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM cost_bucket_summary
WHERE acme_count >= 4
ORDER BY acme_count DESC;
