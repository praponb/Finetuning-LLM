SELECT channel, status, COUNT(*) AS n FROM orders GROUP BY CUBE(channel, status);
