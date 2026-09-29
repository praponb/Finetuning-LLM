SELECT channel FROM orders WHERE status = 'SHIPPED' UNION SELECT channel FROM orders WHERE amount BETWEEN 960 AND 1400 ORDER BY channel DESC;
