WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY age DESC, id) AS dedup_rank
    FROM customers
    QUALIFY dedup_rank = 1
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM accounts x
            WHERE x.customer_id = s.id
              AND x.opened_date >= s.signup_date
        )
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions ch
            WHERE ch.customer_id = s.id
              AND ch.end_date <= s.signup_date
        )
),
eligible_customers AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions x
            WHERE x.customer_id = s.id
              AND x.started_at < s.signup_date
        )
)
-- Grouped metrics
SELECT
    DATE_TRUNC('MONTH', signup_date) AS activity_month,
    COUNT(CASE WHEN country = 'TH' THEN 1 END) AS th_count,
    PERCENTILE_APPROX(age, ARRAY(0.25, 0.5, 0.75)) AS age_quartiles,
    SORT_ARRAY(COLLECT_SET(segment)) AS segment_list,
    STDDEV_SAMP(age) AS age_stddev
FROM eligible_customers
GROUP BY DATE_TRUNC('MONTH', signup_date)
HAVING age_stddev < 0
ORDER BY age_stddev DESC NULLS LAST
