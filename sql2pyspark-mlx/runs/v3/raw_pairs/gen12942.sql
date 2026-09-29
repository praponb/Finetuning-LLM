SELECT
    ss.id,
    ss.product_id,
    ss.revenue,
    p.category,
    p.price,
    s.sqft,
    LAST_DAY(s.opened_date) AS opened_date_month_end
FROM store_sales ss
INNER JOIN products p ON ss.product_id = p.id
INNER JOIN stores s ON ss.store_id = s.id
WHERE ss.sale_date <= '2024-01-13' AND ss.units >= 24
