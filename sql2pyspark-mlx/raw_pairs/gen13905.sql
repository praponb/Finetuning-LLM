SELECT
    a.id,
    a.product_id,
    a.revenue,
    d.cost,
    d.brand
FROM store_sales a
LEFT JOIN products d
    ON a.product_id = d.id;
