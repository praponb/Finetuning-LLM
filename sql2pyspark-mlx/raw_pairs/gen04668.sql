SELECT
    id,
    sale_date,
    store_id,
    RANK() OVER (PARTITION BY MONTH(sale_date), store_id ORDER BY sale_date DESC) sale_date_rank
FROM store_sales
WHERE store_id IN (1, 4, 5, 6)
ORDER BY sale_date_rank DESC;
