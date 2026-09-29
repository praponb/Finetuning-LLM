SELECT
    id,
    opened_date,
    sqft,
    sqft - LAG(sqft, 2) OVER (ORDER BY opened_date, id) AS sqft_change
FROM stores
