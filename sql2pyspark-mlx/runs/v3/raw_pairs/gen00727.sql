WITH valid_stores AS (
    SELECT *
    FROM stores
    WHERE sqft IS NOT NULL
),
summary AS (
    SELECT region, COUNT(sqft) AS sqft_count
    FROM valid_stores
    GROUP BY region
)
SELECT *
FROM summary
WHERE sqft_count >= 3
