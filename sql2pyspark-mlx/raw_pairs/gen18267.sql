SELECT
    *,
    MIN(discount_pct) OVER w_ordered AS running_min_discount_pct,
    COUNT(discount_pct) OVER w_ordered AS running_count_discount_pct,
    SUM(discount_pct) OVER w_ordered AS running_sum_discount_pct
FROM promotions
WHERE starts_on BETWEEN '2023-09-07' AND '2024-03-01'
WINDOW w_ordered AS (PARTITION BY promo_code, channel ORDER BY ends_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
