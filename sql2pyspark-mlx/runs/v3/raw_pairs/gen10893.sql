SELECT channel, COUNT(CASE WHEN channel = 'mobile' THEN 1 END) AS mobile_count, COUNT(amount) AS amount_count FROM orders GROUP BY channel
