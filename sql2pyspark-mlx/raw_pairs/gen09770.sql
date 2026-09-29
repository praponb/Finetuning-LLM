SELECT
    ss.id,
    ss.units,
    ss.product_id,
    ss.revenue,
    s.opened_date,
    s.city,
    s.sqft,
    CASE WHEN s.sqft >= 12663 THEN 'High' WHEN s.sqft >= 2785 THEN 'Medium' ELSE 'Low' END AS sqft_tier
FROM store_sales ss
JOIN stores s ON ss.store_id = s.id;
