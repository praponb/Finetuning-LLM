WITH prepared_listeners AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        username,
        plan,
        country,
        signup_date,
        birth_year,
        interests,
        DATE_FORMAT(signup_date, 'yyyy-MM') AS signup_date_label
    FROM listeners
    WHERE SIZE(interests) >= 1
      AND birth_year < 1986
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LEAD(birth_year, 1, 0) OVER (ORDER BY birth_year, id) AS next_birth_year,
        NTILE(4) OVER (PARTITION BY plan ORDER BY signup_date DESC, id) AS signup_date_quartile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY plan ORDER BY signup_date DESC), 3) AS signup_date_percent_rank,
        MAX(birth_year) OVER (PARTITION BY plan, country) AS plan_country_max_birth_year,
        birth_year - LAG(birth_year) OVER w_all_by_birth_year_2 AS birth_year_change,
        SUM(birth_year) OVER (PARTITION BY plan ORDER BY signup_date DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS birth_year_sum_1_prior,
        LAG(birth_year) OVER (PARTITION BY plan ORDER BY signup_date DESC, id) AS prev_birth_year,
        NTILE(10) OVER (PARTITION BY plan ORDER BY signup_date, id) AS signup_date_decile,
        LEAD(birth_year, 2) OVER (PARTITION BY plan ORDER BY signup_date, id) AS next2_birth_year,
        LAST_VALUE(birth_year) OVER (PARTITION BY plan ORDER BY signup_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_birth_year,
        ROUND(birth_year / NULLIF(SUM(birth_year) OVER (PARTITION BY YEAR(signup_date)), 0), 4) AS birth_year_share_of_signup_date_year,
        COUNT(*) OVER (PARTITION BY YEAR(signup_date)) AS rows_in_signup_date_year,
        ROUND(PERCENT_RANK() OVER w_all_by_birth_year_ties, 3) AS birth_year_percent_rank,
        birth_year - LAG(birth_year) OVER (PARTITION BY plan ORDER BY signup_date DESC, id) AS birth_year_change_2,
        FIRST_VALUE(birth_year) IGNORE NULLS OVER (PARTITION BY plan ORDER BY signup_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_birth_year
    FROM prepared_listeners
    WINDOW w_all_by_birth_year_2 AS (ORDER BY birth_year DESC, id), w_all_by_birth_year_ties AS (ORDER BY birth_year)
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(signup_date_percent_rank) OVER (PARTITION BY signup_date_decile ORDER BY first_known_birth_year ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_signup_date_percent_rank_so_far,
        ROUND(AVG(signup_date_quartile) OVER (PARTITION BY signup_date_quartile), 2) AS signup_date_quartile_avg_signup_date_quartile,
        LEAD(birth_year_change) OVER (PARTITION BY signup_date_decile, MONTH(signup_date) ORDER BY birth_year_percent_rank DESC, id) AS next_birth_year_change,
        ROUND(AVG(birth_year_change) OVER (ORDER BY birth_year DESC NULLS FIRST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS birth_year_change_avg_6_prior,
        MIN(first_known_birth_year) OVER (PARTITION BY signup_date_quartile) AS signup_date_quartile_min_first_known_birth_year,
        ROUND(birth_year_sum_1_prior / NULLIF(SUM(birth_year_sum_1_prior) OVER (PARTITION BY signup_date_quartile), 0), 4) AS birth_year_sum_1_prior_share_of_signup_date_quar
    FROM metrics
)
-- Final projection
SELECT
    id,
    plan,
    country,
    signup_date,
    birth_year,
    interests,
    signup_date_label,
    next_birth_year,
    signup_date_quartile,
    signup_date_percent_rank,
    plan_country_max_birth_year,
    signup_date_decile,
    next2_birth_year,
    birth_year_share_of_signup_date_year,
    rows_in_signup_date_year,
    birth_year_percent_rank,
    birth_year_change_2,
    first_known_birth_year,
    max_signup_date_percent_rank_so_far,
    next_birth_year_change,
    birth_year_change_avg_6_prior,
    signup_date_quartile_min_first_known_birth_year,
    birth_year_sum_1_prior_share_of_signup_date_quar
FROM ranked
ORDER BY signup_date_quartile_min_first_known_birth_year
