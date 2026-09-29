WITH scored AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(discount_pct) OVER (PARTITION BY promo_code ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_discount_pct,
        AVG(discount_pct) OVER (PARTITION BY YEAR(starts_on), promo_code ORDER BY discount_pct, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS discount_pct_avg_6_prior
    FROM promotions
)
-- Aggregate with conditional metrics
SELECT
    channel,
    AVG(discount_pct_avg_6_prior) FILTER (WHERE promo_code = 'WELCOME5') AS welcome5_avg_discount_pct_avg_6_prior,
    STDDEV_SAMP(discount_pct) AS discount_pct_stddev
FROM scored
GROUP BY 1
HAVING AVG(discount_pct_avg_6_prior) FILTER (WHERE promo_code = 'WELCOME5') >= 0
