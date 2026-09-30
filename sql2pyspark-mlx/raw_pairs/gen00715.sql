SELECT QUARTER(s.opened_date) opened_quarter, MAX(ss.revenue) - MIN(ss.revenue) revenue_range
FROM store_sales ss
LEFT OUTER JOIN stores s ON ss.store_id = s.id
WHERE ss.revenue >= 2300
GROUP BY QUARTER(s.opened_date);
