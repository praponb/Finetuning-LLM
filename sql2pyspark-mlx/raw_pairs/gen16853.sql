SELECT
    id,
    sqft,
    region,
    opened_date,
    RANK() OVER (ORDER BY sqft DESC) AS sqft_rank,
    LAST_VALUE(sqft) OVER (PARTITION BY region ORDER BY opened_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_sqft
FROM stores
WHERE opened_date BETWEEN '2023-06-01' AND '2024-05-02';
