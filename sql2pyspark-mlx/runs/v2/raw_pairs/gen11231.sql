SELECT
    id,
    product_id,
    store_id,
    sale_date,
    units,
    PERCENT_RANK() OVER (PARTITION BY product_id, store_id ORDER BY sale_date) AS sale_date_pct_rank,
    PERCENT_RANK() OVER (PARTITION BY store_id ORDER BY units DESC) AS units_pct_rank
FROM store_sales;
