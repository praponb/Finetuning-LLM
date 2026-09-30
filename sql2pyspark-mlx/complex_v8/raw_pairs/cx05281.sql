WITH filtered AS (
    -- Filter listeners
    SELECT
        id,
        plan,
        country,
        signup_date,
        birth_year,
        interests,
        YEAR(signup_date) AS signup_date_year
    FROM listeners
    WHERE username IS NOT NULL
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_all_by_signup_date_ties, 3) AS signup_date_cume_dist,
        ROW_NUMBER() OVER w_signup_date_year_signup_date AS seq_no,
        FIRST_VALUE(signup_date) IGNORE NULLS OVER (ORDER BY birth_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_signup_date,
        DENSE_RANK() OVER (ORDER BY birth_year DESC, id) AS birth_year_dense_rank,
        RANK() OVER (PARTITION BY YEAR(signup_date), signup_date_year ORDER BY birth_year) AS birth_year_rank,
        LAST_VALUE(birth_year) OVER (ORDER BY birth_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_birth_year,
        MIN(birth_year) OVER (PARTITION BY plan, country) AS plan_country_min_birth_year,
        FIRST_VALUE(birth_year) OVER (ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_birth_year,
        ROUND(PERCENT_RANK() OVER w_all_by_birth_year_ties, 3) AS birth_year_percent_rank,
        LAST_VALUE(signup_date) OVER (PARTITION BY YEAR(signup_date), signup_date_year ORDER BY birth_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_signup_date
    FROM filtered
    WINDOW w_all_by_signup_date_ties AS (ORDER BY signup_date), w_signup_date_year_signup_date AS (PARTITION BY YEAR(signup_date), signup_date_year ORDER BY birth_year, id), w_all_by_birth_year_ties AS (ORDER BY birth_year)
),
first_rows AS (
    -- Top 5 per group
    SELECT *
    FROM with_trends
    WHERE seq_no <= 5
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        plan,
        country,
        signup_date,
        birth_year,
        signup_date_cume_dist,
        seq_no,
        birth_year_dense_rank,
        first_birth_year,
        birth_year_percent_rank,
        last_signup_date,
        SIZE(COLLECT_SET(country) OVER w_plan_birth_year_rank) AS distinct_country_in_plan_birth_year_rank,
        COUNT(*) OVER (PARTITION BY plan, birth_year_rank) AS rows_in_plan_birth_year_rank,
        LAST_VALUE(first_birth_year) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', last_signup_date) ORDER BY seq_no NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_first_birth_year,
        NTH_VALUE(birth_year_percent_rank, 2) OVER (PARTITION BY signup_date_year ORDER BY last_signup_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_birth_year_percent_rank,
        ROUND(signup_date_cume_dist - AVG(signup_date_cume_dist) OVER (PARTITION BY plan, birth_year_rank), 2) AS signup_date_cume_dist_vs_plan_birth_year_rank_av,
        ROUND(AVG(birth_year_dense_rank) OVER (PARTITION BY plan, birth_year_rank), 2) AS plan_birth_year_rank_avg_birth_year_dense_rank
    FROM first_rows
    WINDOW w_plan_birth_year_rank AS (PARTITION BY plan, birth_year_rank)
)
-- Final projection
SELECT
    id,
    country,
    signup_date,
    signup_date_cume_dist,
    seq_no,
    birth_year_dense_rank,
    distinct_country_in_plan_birth_year_rank,
    last_known_first_birth_year,
    second_birth_year_percent_rank,
    signup_date_cume_dist_vs_plan_birth_year_rank_av,
    plan_birth_year_rank_avg_birth_year_dense_rank,
    CONCAT(country, '#', CAST(birth_year AS STRING)) AS country_ref,
    ABS(birth_year - 1977) AS birth_year_distance
FROM metrics
ORDER BY plan_birth_year_rank_avg_birth_year_dense_rank NULLS LAST, signup_date_cume_dist_vs_plan_birth_year_rank_av DESC
