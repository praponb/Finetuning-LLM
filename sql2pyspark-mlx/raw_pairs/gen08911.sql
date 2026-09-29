WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft DESC, id) AS rnk
    FROM stores
)
SELECT *
FROM numbered
WHERE rnk = 1;
