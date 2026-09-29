SELECT channel FROM orders WHERE discount IS NOT NULL UNION ALL SELECT channel FROM orders WHERE shipping_fee >= 33 ORDER BY channel DESC;
