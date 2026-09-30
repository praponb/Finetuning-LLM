SELECT YEAR(s.opened_date) AS opened_year, COUNT(i.quantity) AS quantity_count
FROM inventory i
JOIN stores s ON i.store_id = s.id
JOIN products p ON p.id = i.product_id
GROUP BY YEAR(s.opened_date);
