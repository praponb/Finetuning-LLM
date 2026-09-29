WITH prepared_listeners AS (
    -- Filter listeners
    SELECT
        id,
        plan,
        country,
        signup_date,
        interests,
        DATEDIFF(DATE '2025-06-30', signup_date) AS days_since_signup_date,
        LENGTH(TRIM(username)) AS username_len,
        DATEDIFF(DATE '2025-06-30', signup_date) AS days_since_signup_date_2
    FROM listeners
    WHERE birth_year > 1969
      AND country IS NOT NULL
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT ROUND(AVG(p.ms_played), 2) FROM plays p WHERE p.listener_id = s.id) AS avg_play_ms_played
    FROM prepared_listeners s
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MIN(p.played_at) FROM plays p WHERE p.listener_id = s.id) AS first_played_at,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.listener_id = s.id) AS last_played_at
    FROM summarized_rows s
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(days_since_signup_date) OVER (ORDER BY last_played_at DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS days_since_signup_date_sum_3_prior,
        NTH_VALUE(avg_play_ms_played, 2) OVER (ORDER BY last_played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_avg_play_ms_played
    FROM profiled
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, value
FROM (
    SELECT id, days_since_signup_date, days_since_signup_date_sum_3_prior, days_since_signup_date_2, username_len
    FROM sequenced
) src
UNPIVOT INCLUDE NULLS (
    value FOR field IN (days_since_signup_date, days_since_signup_date_sum_3_prior, days_since_signup_date_2, username_len)
) unp
