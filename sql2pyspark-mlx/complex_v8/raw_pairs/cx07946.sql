WITH base_listeners AS (
    -- Filter listeners
    SELECT
        id,
        username,
        plan,
        interests,
        ARRAY_CONTAINS(interests, 'kpop') AS has_kpop,
        LOWER(REGEXP_REPLACE(TRIM(country), ' ', '_')) AS country_slug,
        FLOOR(birth_year / 10) * 10 AS birth_year_bucket
    FROM listeners
    WHERE plan = 'premium'
      AND signup_date IS NOT NULL
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(birth_year_bucket) OVER (PARTITION BY plan ORDER BY birth_year_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_birth_year_bucket,
        ROUND(birth_year_bucket - AVG(birth_year_bucket) OVER (PARTITION BY plan, has_kpop), 2) AS birth_year_bucket_vs_plan_has_kpop_avg,
        ROUND(AVG(birth_year_bucket) OVER (PARTITION BY has_kpop ORDER BY birth_year_bucket DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS birth_year_bucket_avg_3,
        NTH_VALUE(birth_year_bucket, 2) OVER (PARTITION BY plan ORDER BY birth_year_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_birth_year_bucket,
        ROUND(PERCENT_RANK() OVER w_plan_by_birth_year_bucket_ti, 3) AS birth_year_bucket_percent_rank
    FROM base_listeners
    WINDOW w_plan_by_birth_year_bucket_ti AS (PARTITION BY plan ORDER BY birth_year_bucket)
),
exploded AS (
    -- One row per interest
    SELECT
        s.id,
        s.username,
        s.plan,
        s.has_kpop,
        s.country_slug,
        s.birth_year_bucket,
        s.first_birth_year_bucket,
        s.birth_year_bucket_vs_plan_has_kpop_avg,
        s.birth_year_bucket_avg_3,
        s.second_birth_year_bucket,
        s.birth_year_bucket_percent_rank,
        t.interest_pos,
        t.interest
    FROM sequenced s
    LATERAL VIEW OUTER POSEXPLODE(s.interests) t AS interest_pos, interest
)
SELECT
    id,
    username,
    plan,
    has_kpop,
    country_slug,
    birth_year_bucket,
    first_birth_year_bucket,
    birth_year_bucket_vs_plan_has_kpop_avg,
    birth_year_bucket_percent_rank,
    interest_pos,
    interest
FROM exploded
ORDER BY plan DESC
