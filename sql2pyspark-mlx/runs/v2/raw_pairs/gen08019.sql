SELECT channel FROM orders WHERE discount BETWEEN 0.19 AND 0.27 UNION ALL SELECT channel FROM orders WHERE amount >= 750 ORDER BY channel;
