SELECT
    store_id,
    product_id,
    ADD_MONTHS(sale_date, 1) AS sale_date_plus_months,
    LAST_DAY(sale_date) AS sale_date_month_end
FROM store_sales
WHERE units < 27 AND revenue <= 2600 AND store_id IN (1, 2, 4, 6);
