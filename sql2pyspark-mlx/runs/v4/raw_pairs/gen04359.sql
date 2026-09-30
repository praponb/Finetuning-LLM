WITH with_windows AS (
    SELECT
        *,
        LEAD(discount_pct, 1) OVER (PARTITION BY channel ORDER BY starts_on, id) AS next_discount_pct,
        DENSE_RANK() OVER (ORDER BY discount_pct) AS discount_pct_dense_rank,
        NTILE(5) OVER (PARTITION BY channel ORDER BY discount_pct DESC, id) AS discount_pct_bucket
    FROM promotions
)
SELECT
    channel,
    DATE_TRUNC('YEAR', starts_on) AS year_start,
    CASE WHEN next_discount_pct >= 0.32 THEN 'PREMIUM' ELSE 'STANDARD' END AS next_discount_pct_tier,
    COUNT(CASE WHEN channel = 'store' THEN 1 END) AS store_count,
    COUNT(*) FILTER (WHERE channel = 'store') AS store_rows,
    GROUPING_ID() AS gid
FROM with_windows
GROUP BY GROUPING SETS ((channel, DATE_TRUNC('YEAR', starts_on), CASE WHEN next_discount_pct >= 0.32 THEN 'PREMIUM' ELSE 'STANDARD' END), (channel, DATE_TRUNC('YEAR', starts_on)), ());
