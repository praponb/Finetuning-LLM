WITH scored AS (
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        CUME_DIST() OVER (PARTITION BY MONTH(signup_date), segment ORDER BY credit_limit ASC NULLS FIRST) credit_limit_cume_dist,
        SUM(age) OVER (PARTITION BY segment, city) segment_sum_age,
        LAST_VALUE(segment, TRUE) OVER (ORDER BY signup_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_segment
    FROM customers
)
SELECT
    DATE_TRUNC('QUARTER', signup_date) AS quarter_start,
    CASE WHEN age >= 71 THEN 'PREMIUM' ELSE 'STANDARD' END AS age_tier,
    COUNT_IF(city = 'Singapore') AS singapore_events,
    BIT_AND(age) AS age_bit_and,
    BIT_OR(age) AS age_bit_or,
    COUNT_IF(segment = 'Corporate') AS corporate_events,
    COUNT(*) FILTER (WHERE segment = 'Corporate') AS corporate_rows,
    GROUPING_ID() AS grouping_level
FROM scored
GROUP BY ROLLUP (DATE_TRUNC('QUARTER', signup_date), CASE WHEN age >= 71 THEN 'PREMIUM' ELSE 'STANDARD' END)
HAVING corporate_events > 1;
