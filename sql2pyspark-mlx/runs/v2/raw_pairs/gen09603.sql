SELECT region, DATEDIFF(CURRENT_DATE(), opened_date) AS days_since_opened_date
FROM stores
ORDER BY region DESC
