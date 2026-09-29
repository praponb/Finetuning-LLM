WITH metrics AS (
    SELECT
        *,
        MAX(discount_pct) OVER w_hist AS last3_max_discount_pct,
        COUNT(discount_pct) OVER w_hist AS last3_count_discount_pct,
        SUM(discount_pct) OVER w_hist AS last3_sum_discount_pct,
        LAG(discount_pct, 1, 0) OVER w_ordered AS prev_discount_pct
    FROM promotions
    WINDOW w_hist AS (PARTITION BY channel, promo_code ORDER BY discount_pct, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY promo_code ORDER BY starts_on, id)
)
SELECT
    channel,
    promo_code,
    COUNT(*) FILTER (WHERE promo_code = 'VIP') AS vip_rows,
    COUNT(*) AS num_rows,
    COUNT(CASE WHEN promo_code = 'VIP' THEN 1 END) AS vip_count,
    MIN(last3_max_discount_pct) AS min_last3_max_discount_pct,
    SORT_ARRAY(COLLECT_SET(promo_code)) AS promo_code_list,
    DENSE_RANK() OVER (PARTITION BY channel ORDER BY COUNT(*) FILTER (WHERE promo_code = 'VIP') DESC) AS vip_rows_dense_rank
FROM metrics
GROUP BY channel, promo_code
ORDER BY num_rows
