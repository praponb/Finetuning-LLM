SELECT
    id,
    store_id,
    revenue,
    product_id,
    units,
    sale_date,
    ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY revenue, id) AS revenue_row,
    LAST_VALUE(sale_date) OVER (PARTITION BY product_id, store_id ORDER BY units DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_sale_date
FROM store_sales;
