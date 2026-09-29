SELECT
    id,
    units,
    store_id,
    units * revenue AS revenue_per_unit
FROM store_sales;
