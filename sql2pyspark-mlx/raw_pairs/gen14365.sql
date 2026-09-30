WITH deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY segment, city ORDER BY signup_date DESC NULLS FIRST, id) AS row_num
    FROM customers
    QUALIFY row_num = 1
)
-- Multi-level aggregation
SELECT
    CASE WHEN credit_limit >= 15800 THEN 'PREMIUM' ELSE 'STANDARD' END AS credit_limit_tier,
    city,
    CASE WHEN age >= 59 THEN 'HIGH' ELSE 'LOW' END AS age_tier,
    COUNT_IF(country = 'UK') AS uk_events,
    SUM(CASE WHEN country = 'US' THEN age ELSE 0 END) AS us_age
FROM deduped
GROUP BY CASE WHEN credit_limit >= 15800 THEN 'PREMIUM' ELSE 'STANDARD' END, city, CASE WHEN age >= 59 THEN 'HIGH' ELSE 'LOW' END
ORDER BY age_tier DESC NULLS LAST;
