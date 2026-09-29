WITH deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY credit_limit DESC NULLS LAST, id) AS rn
        FROM customers
    ) ranked
    WHERE rn = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(credit_limit) IGNORE NULLS OVER (PARTITION BY country, city ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_credit_limit,
        SUM(age) OVER (ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_age
    FROM deduped
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(running_age, 1) OVER (PARTITION BY country, city ORDER BY age, id) AS next_running_age,
        DENSE_RANK() OVER (ORDER BY age DESC NULLS LAST) AS age_dense_rank,
        NTILE(10) OVER w_ordered AS credit_limit_bucket,
        SUM(age) OVER w_part AS running_age_2
    FROM enriched
    WINDOW w_ordered AS (PARTITION BY country ORDER BY credit_limit, id), w_part AS (PARTITION BY country ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM subscriptions ch
            WHERE ch.customer_id = s.id
              AND ch.start_date <= s.signup_date
        )
)
-- Grouped metrics
SELECT
    CASE WHEN age >= 68 THEN 'LARGE' ELSE 'SMALL' END AS age_tier,
    COUNT(DISTINCT city) AS distinct_cities,
    COUNT_IF(country = 'JP') AS jp_events,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    COUNT(*) AS row_count
FROM eligible
GROUP BY CASE WHEN age >= 68 THEN 'LARGE' ELSE 'SMALL' END
HAVING COUNT_IF(country = 'JP') >= 1
ORDER BY row_count DESC
