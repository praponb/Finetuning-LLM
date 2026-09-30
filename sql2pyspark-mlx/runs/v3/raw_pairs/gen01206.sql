SELECT *
FROM (
    SELECT
        store_id,
        product_id,
        MIN(sale_date) min_sale_date,
        ROUND(STDDEV(units), 2) stddev_units
    FROM store_sales
    GROUP BY store_id, product_id
) sub
WHERE stddev_units < 23;
