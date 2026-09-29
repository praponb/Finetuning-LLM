SELECT id, plan, end_date
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY end_date DESC, id) pos
    FROM subscriptions
    WHERE status = 'ACTIVE'
) ranked
WHERE pos <= 2;
