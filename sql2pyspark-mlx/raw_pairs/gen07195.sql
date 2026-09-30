WITH kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM customers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions c
            WHERE c.customer_id = s.id
              AND c.started_at >= s.signup_date
        )
),
kept_customers AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE EXISTS (
            SELECT 1
            FROM sessions x
            WHERE x.customer_id = s.id
              AND x.started_at <= s.signup_date
        )
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY age DESC, id) AS rank_in_group
        FROM kept_customers
    ) ranked
    WHERE rank_in_group = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        name,
        signup_date,
        is_active,
        credit_limit,
        age,
        MAX(credit_limit) OVER (PARTITION BY country, city ORDER BY rank_in_group, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_max_credit_limit,
        MIN(credit_limit) OVER (PARTITION BY country, city ORDER BY rank_in_group, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_min_credit_limit,
        COUNT(credit_limit) OVER (PARTITION BY country, city ORDER BY rank_in_group, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) last3_count_credit_limit,
        AVG(credit_limit) OVER (PARTITION BY country) country_avg_credit_limit
    FROM latest
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        is_active,
        credit_limit,
        last3_max_credit_limit,
        last3_min_credit_limit,
        RANK() OVER (ORDER BY last3_max_credit_limit NULLS LAST) last3_max_credit_limit_rank,
        LEAD(age, 2) OVER w_grp next_age,
        CUME_DIST() OVER (ORDER BY age DESC) age_cume_dist
    FROM scored
    WHERE credit_limit < 5100
    WINDOW w_grp AS (ORDER BY country_avg_credit_limit, id)
)
-- Grouped metrics
SELECT
    CASE WHEN next_age >= 67 THEN 'LARGE' ELSE 'SMALL' END AS next_age_tier,
    APPROX_PERCENTILE(age_cume_dist, ARRAY(0.25, 0.5, 0.75)) AS age_cume_dist_quartiles,
    PERCENTILE_APPROX(last3_max_credit_limit, 0.95) AS last3_max_credit_limit_p95,
    COUNT(*) AS event_count,
    COUNT(DISTINCT id) AS distinct_ids
FROM enriched
GROUP BY CASE WHEN next_age >= 67 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY distinct_ids
