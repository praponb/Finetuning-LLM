SELECT
    id,
    store_id,
    product_id,
    sale_date,
    revenue,
    COUNT(units) OVER w_part AS sale_date_year_count_units,
    COUNT(revenue) OVER (PARTITION BY product_id) AS product_id_count_revenue,
    DENSE_RANK() OVER (PARTITION BY store_id, product_id ORDER BY units NULLS LAST) AS units_dense_rank,
    ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY units, id) AS event_seq
FROM store_sales
WINDOW w_part AS (PARTITION BY YEAR(sale_date));
