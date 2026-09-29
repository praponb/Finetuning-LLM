SELECT *, DATEDIFF(CURRENT_DATE(), last_restock) AS days_since_last_restock FROM inventory WHERE YEAR(last_restock) >= 2024;
