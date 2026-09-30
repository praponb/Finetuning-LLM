WITH metrics AS (
    SELECT
        *,
        SUM(discount_pct) OVER (ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_discount_pct,
        SUM(discount_pct) OVER (PARTITION BY channel, promo_code ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_discount_pct_2,
        AVG(discount_pct) OVER (PARTITION BY YEAR(starts_on), promo_code ORDER BY ends_on, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS discount_pct_avg_5,
        PERCENT_RANK() OVER (PARTITION BY promo_code, channel ORDER BY discount_pct DESC) AS discount_pct_percent_rank
    FROM promotions
)
SELECT
    id,
    starts_on,
    ends_on,
    discount_pct,
    running_discount_pct,
    discount_pct_avg_5,
    discount_pct_percent_rank
FROM metrics
ORDER BY discount_pct_percent_rank DESC NULLS LAST, running_discount_pct
