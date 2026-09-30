SELECT channel, ROUND(STDDEV(amount), 2) AS stddev_amount
FROM orders
WHERE discount >= 0.25
GROUP BY channel
ORDER BY channel DESC, stddev_amount DESC;
