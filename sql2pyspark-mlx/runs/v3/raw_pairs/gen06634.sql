SELECT channel, MAX(amount) - MIN(amount) amount_range FROM orders WHERE status IN ('SHIPPED', 'PENDING', 'RETURNED') AND discount >= 0.24 GROUP BY channel HAVING MAX(amount) - MIN(amount) >= 44;
