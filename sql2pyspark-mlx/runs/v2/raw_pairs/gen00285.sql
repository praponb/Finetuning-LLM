SELECT id, paid_at, amount, SUM(amount) OVER (ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount FROM payments WHERE amount < 920 ORDER BY paid_at;
