SELECT status, last_opened_date, frozen_count
FROM (
    SELECT status, MAX(opened_date) AS last_opened_date, SUM(CASE WHEN status = 'FROZEN' THEN 1 ELSE 0 END) AS frozen_count
    FROM accounts
    WHERE balance > 45000
    GROUP BY status
) AS agg
WHERE frozen_count >= 21;
