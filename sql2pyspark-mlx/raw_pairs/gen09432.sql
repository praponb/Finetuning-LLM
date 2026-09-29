SELECT
    YEAR(paid_at) AS paid_year,
    status,
    order_id,
    MAX(amount) AS max_amount,
    MAX(amount) - MIN(amount) AS amount_range
FROM payments
GROUP BY paid_year, status, order_id
HAVING MAX(amount) >= 710
ORDER BY paid_year DESC, status, order_id
LIMIT 1;
