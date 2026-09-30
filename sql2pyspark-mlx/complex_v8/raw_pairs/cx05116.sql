WITH recent_listeners AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        username,
        plan,
        country,
        CASE WHEN birth_year >= 1980 THEN 'HIGH' WHEN birth_year >= 1975 THEN 'MEDIUM' ELSE 'LOW' END AS birth_year_tier,
        ADD_MONTHS(signup_date, 6) AS signup_date_plus_6m
    FROM listeners
    WHERE YEAR(signup_date) = 2025
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        plan,
        country,
        birth_year_tier,
        ROW_NUMBER() OVER (PARTITION BY birth_year_tier, country ORDER BY signup_date_plus_6m NULLS LAST, id) AS signup_date_plus_6m_seq,
        DATEDIFF(signup_date_plus_6m, LAG(signup_date_plus_6m) OVER (PARTITION BY birth_year_tier, country ORDER BY signup_date_plus_6m NULLS LAST, id)) AS days_since_prev_signup_date_plus_6m,
        DENSE_RANK() OVER w_birth_year_tier_country_by_s_2 AS signup_date_plus_6m_dense_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY signup_date_plus_6m), 3) AS signup_date_plus_6m_percent_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY signup_date_plus_6m, id), 3) AS signup_date_plus_6m_percent_rank_2,
        ROUND(CUME_DIST() OVER w_birth_year_tier_country_by_s_2, 3) AS signup_date_plus_6m_cume_dist,
        DATEDIFF(signup_date_plus_6m, LAG(signup_date_plus_6m) OVER (ORDER BY signup_date_plus_6m, id)) AS days_since_prev_signup_date_plus_6m_2,
        NTH_VALUE(signup_date_plus_6m, 2) OVER (ORDER BY signup_date_plus_6m, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_signup_date_plus_6m,
        LAG(signup_date_plus_6m) OVER (PARTITION BY plan ORDER BY signup_date_plus_6m, id) AS prev_signup_date_plus_6m
    FROM recent_listeners
    WINDOW w_birth_year_tier_country_by_s_2 AS (PARTITION BY birth_year_tier, country ORDER BY signup_date_plus_6m NULLS LAST)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY signup_date_plus_6m_seq, plan ORDER BY signup_date_plus_6m_seq DESC NULLS FIRST), 3) AS signup_date_plus_6m_seq_percent_rank,
        NTH_VALUE(prev_signup_date_plus_6m, 2) OVER (ORDER BY signup_date_plus_6m_percent_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_prev_signup_date_plus_6m,
        ROUND(AVG(signup_date_plus_6m_seq) OVER (PARTITION BY signup_date_plus_6m_seq, plan ORDER BY signup_date_plus_6m_seq DESC NULLS FIRST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS signup_date_plus_6m_seq_avg_4
    FROM with_windows
)
SELECT
    id,
    country,
    signup_date_plus_6m_seq,
    days_since_prev_signup_date_plus_6m,
    signup_date_plus_6m_dense_rank,
    signup_date_plus_6m_percent_rank_2,
    signup_date_plus_6m_cume_dist,
    days_since_prev_signup_date_plus_6m_2,
    second_signup_date_plus_6m,
    prev_signup_date_plus_6m,
    second_prev_signup_date_plus_6m,
    signup_date_plus_6m_seq_avg_4
FROM metrics
WHERE days_since_prev_signup_date_plus_6m >= 80
ORDER BY second_prev_signup_date_plus_6m NULLS LAST, signup_date_plus_6m_cume_dist
