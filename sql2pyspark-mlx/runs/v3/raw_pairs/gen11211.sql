SELECT
    product_id,
    sale_date,
    units,
    LAST_DAY(sale_date) sale_date_month_end,
    ROUND(units / revenue, 2) revenue_per_unit
FROM store_sales
ORDER BY units DESC;
