SELECT *, DATEDIFF(CURRENT_DATE(), opened_date) AS days_since_opened_date FROM stores ORDER BY days_since_opened_date DESC;
