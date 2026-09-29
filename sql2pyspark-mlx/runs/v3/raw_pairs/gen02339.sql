SELECT
    id,
    city,
    rating,
    DENSE_RANK() OVER (PARTITION BY city ORDER BY rating DESC) dense_rnk
FROM suppliers
WHERE country IN ('US', 'UK', 'JP')
