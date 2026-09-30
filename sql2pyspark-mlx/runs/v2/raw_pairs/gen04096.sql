SELECT channel, status, COUNT(DISTINCT discount) AS unique_customers FROM orders GROUP BY channel, status;
