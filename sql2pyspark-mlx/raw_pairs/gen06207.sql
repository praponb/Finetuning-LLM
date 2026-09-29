SELECT ROUND(STDDEV(amount), 2) AS stddev_amount
FROM transactions
WHERE is_flagged = FALSE OR merchant IN ('Lazada', 'Amazon', 'Grab')
ORDER BY stddev_amount DESC;
