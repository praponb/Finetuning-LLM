SELECT plan, paused_count, first_start_date
FROM (
    SELECT plan, SUM(CASE WHEN status = 'PAUSED' THEN 1 ELSE 0 END) paused_count, MIN(start_date) first_start_date
    FROM subscriptions
    GROUP BY plan
) x
WHERE paused_count < 78;
