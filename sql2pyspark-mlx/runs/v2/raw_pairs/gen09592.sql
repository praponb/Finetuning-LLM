SELECT
    ss.id,
    ss.revenue,
    p.supplier_id,
    p.launch_date,
    p.cost,
    ss.revenue * ss.units AS revenue_per_unit
FROM store_sales ss
INNER JOIN products p ON ss.product_id = p.id;
