SELECT
    id,
    opened_date,
    region,
    sqft,
    ROW_NUMBER() OVER (ORDER BY opened_date DESC, id) AS row_num,
    ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft, id) AS rn
FROM stores
ORDER BY opened_date DESC;
