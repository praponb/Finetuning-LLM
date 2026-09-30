SELECT
    id,
    product_id,
    store_id,
    revenue,
    sale_date,
    RANK() OVER (PARTITION BY product_id, store_id ORDER BY revenue) AS revenue_rank,
    RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', sale_date) ORDER BY sale_date DESC) AS sale_date_rank
FROM store_sales
WHERE revenue BETWEEN 1500 AND 3800
ORDER BY id DESC, 4;
