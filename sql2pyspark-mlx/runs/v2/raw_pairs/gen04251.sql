SELECT
    ss.units,
    ss.sale_date,
    p.cost,
    s.opened_date
FROM store_sales ss
INNER JOIN products p ON ss.product_id = p.id
INNER JOIN stores s ON ss.store_id = s.id
WHERE ss.sale_date <= '2025-09-15'
