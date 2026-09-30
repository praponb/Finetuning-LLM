SELECT opened_month, min_opened_date, balance_count
FROM (
    SELECT DATE_FORMAT(opened_date, 'yyyy-MM') opened_month, MIN(opened_date) min_opened_date, COUNT(balance) balance_count
    FROM accounts
    GROUP BY DATE_FORMAT(opened_date, 'yyyy-MM')
) agg
WHERE balance_count < 9;
