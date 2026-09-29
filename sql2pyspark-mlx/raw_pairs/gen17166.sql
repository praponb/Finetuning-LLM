SELECT
    id,
    city,
    sqft,
    NTILE(4) OVER (PARTITION BY city ORDER BY sqft DESC, id) AS sqft_ntile
FROM stores
ORDER BY 2 DESC, sqft_ntile DESC
