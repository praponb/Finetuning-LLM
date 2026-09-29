WITH with_windows AS (
    SELECT
        *,
        MIN(age) OVER (PARTITION BY city ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_min_age,
        MAX(age) OVER (PARTITION BY city ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_max_age,
        SUM(age) OVER (PARTITION BY city ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_sum_age
    FROM customers
    WHERE name LIKE 'bo%'
)
SELECT
    city,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT_IF(country = 'TH') AS th_events
FROM with_windows
GROUP BY city
ORDER BY th_events NULLS FIRST
