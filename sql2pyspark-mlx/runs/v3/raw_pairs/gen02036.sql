SELECT DATE_FORMAT(stores.opened_date, 'yyyy-MM') opened_month, COUNT(store_sales.units) units_count
FROM store_sales
JOIN stores ON store_sales.store_id = stores.id
WHERE stores.city IN ('Tokyo', 'Berlin')
GROUP BY DATE_FORMAT(stores.opened_date, 'yyyy-MM');
