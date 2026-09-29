WITH with_windows AS (
    SELECT
        *,
        MAX(age) OVER w_recent AS running_max_age,
        MIN(age) OVER w_recent AS running_min_age,
        SUM(age) OVER w_recent AS running_sum_age,
        NTILE(3) OVER (PARTITION BY city ORDER BY credit_limit DESC, id) AS credit_limit_bucket
    FROM customers
    WINDOW w_recent AS (PARTITION BY segment ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    segment,
    COUNT(*) FILTER (WHERE country = 'SG') AS sg_rows,
    ROUND(SUM(credit_limit_bucket) / NULLIF(COUNT(CASE WHEN city = 'Chiang Mai' THEN 1 END), 0), 2) AS credit_limit_bucket_per_chiang_mai,
    COUNT(*) AS row_count,
    AVG(running_max_age) AS avg_running_max_age
FROM with_windows
GROUP BY segment
ORDER BY segment DESC NULLS FIRST;
