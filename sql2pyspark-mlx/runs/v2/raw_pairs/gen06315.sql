SELECT
    ss.product_id,
    ss.revenue,
    s.store_name,
    s.opened_date,
    s.sqft
FROM store_sales ss
FULL OUTER JOIN stores s ON ss.store_id = s.id
WHERE ss.revenue > 1600;
