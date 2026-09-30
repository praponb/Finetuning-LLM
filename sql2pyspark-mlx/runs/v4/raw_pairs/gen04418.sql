WITH scored AS (
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        SUM(discount_pct) OVER (PARTITION BY channel ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_discount_pct,
        NTILE(3) OVER (ORDER BY discount_pct, id) AS discount_pct_bucket
    FROM promotions
)
SELECT
    id,
    channel,
    starts_on,
    discount_pct,
    cumulative_discount_pct,
    discount_pct_bucket,
    CASE WHEN channel RLIKE '^(partner|web)$' THEN 'OTHER_ZONE' WHEN channel RLIKE '^(mobile|store)$' THEN 'GROUP_B' ELSE 'CORE' END AS channel_zone
FROM scored
ORDER BY cumulative_discount_pct;
