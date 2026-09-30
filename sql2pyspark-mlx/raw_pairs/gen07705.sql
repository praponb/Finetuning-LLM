WITH base AS (
    SELECT *, LAST_DAY(ends_on) AS ends_on_month_end
    FROM promotions
    WHERE ends_on BETWEEN '2023-08-01' AND '2025-06-01'
)
SELECT
    *,
    AVG(discount_pct) OVER (ORDER BY discount_pct, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS discount_pct_avg_5,
    ROW_NUMBER() OVER (PARTITION BY promo_code, channel ORDER BY discount_pct, id) AS event_seq,
    ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY promo_code), 4) AS discount_pct_share
FROM base;
