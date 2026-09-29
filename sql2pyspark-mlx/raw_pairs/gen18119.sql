SELECT channel, MAX(discount) AS max_discount FROM orders GROUP BY channel HAVING MAX(amount) > 580 ORDER BY channel DESC
