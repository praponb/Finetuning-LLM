SELECT
    ss.id,
    ss.units,
    s.opened_date,
    p.launch_date
FROM store_sales ss
FULL OUTER JOIN stores s ON ss.store_id = s.id
JOIN products p ON ss.product_id = p.id
ORDER BY units DESC, launch_date ASC;
