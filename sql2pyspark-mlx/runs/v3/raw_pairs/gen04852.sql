SELECT DATE_FORMAT(opened_date, 'yyyy-MM') opened_month, SORT_ARRAY(COLLECT_SET(status)) status_list, ROUND((100 * SUM(CASE WHEN account_type = 'LOAN' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_loan
FROM accounts
WHERE account_type != 'CHECKING' AND customer_id IN (26, 36)
GROUP BY DATE_FORMAT(opened_date, 'yyyy-MM')
ORDER BY opened_month, pct_loan DESC
