SELECT
    *,
    MIN(amount) OVER w_part AS running_min_amount,
    AVG(amount) OVER w_part AS running_avg_amount,
    SUM(amount) OVER w_part AS running_sum_amount,
    MAX(amount) OVER w_part AS running_max_amount,
    FIRST_VALUE(amount) IGNORE NULLS OVER (PARTITION BY merchant, txn_type ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount
FROM transactions
WHERE merchant IN ('Shell', 'Lazada', '7-Eleven')
WINDOW w_part AS (PARTITION BY merchant ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
