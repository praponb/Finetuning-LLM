SELECT DATE_FORMAT(stores.opened_date, 'yyyy-MM') opened_month, COUNT(*) cnt, SUM(inventory.quantity) total_quantity
FROM inventory
LEFT JOIN stores ON stores.id = inventory.store_id
WHERE stores.sqft >= 10899
GROUP BY DATE_FORMAT(stores.opened_date, 'yyyy-MM')
HAVING cnt >= 2
ORDER BY cnt ASC, opened_month ASC
LIMIT 5;
