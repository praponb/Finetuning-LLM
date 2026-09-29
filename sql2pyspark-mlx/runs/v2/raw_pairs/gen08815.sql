SELECT channel, total_count
FROM (
    SELECT channel, COUNT(*) total_count
    FROM orders
    GROUP BY channel
) agg
WHERE total_count >= 3
ORDER BY total_count ASC, channel ASC
