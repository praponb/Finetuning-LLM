SELECT
    id,
    product_id,
    sale_date,
    PERCENT_RANK() OVER (PARTITION BY product_id ORDER BY sale_date) AS sale_date_pct_rank
FROM store_sales
ORDER BY id;
