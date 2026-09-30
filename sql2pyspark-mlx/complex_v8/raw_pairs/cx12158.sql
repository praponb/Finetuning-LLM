WITH recent_listeners AS (
    -- Filter listeners
    SELECT
        id,
        username,
        signup_date,
        birth_year,
        interests
    FROM listeners
    WHERE birth_year >= 1975
      AND LOWER(username) LIKE '%_m%'
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(birth_year) OVER (ORDER BY signup_date, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS birth_year_max_1_prior,
        LEAD(birth_year, 2) OVER w_all_by_signup_date AS next2_birth_year
    FROM recent_listeners
    WINDOW w_all_by_signup_date AS (ORDER BY signup_date, id)
),
exploded AS (
    -- One row per interest
    SELECT
        s.id,
        s.username,
        s.signup_date,
        s.birth_year,
        s.birth_year_max_1_prior,
        s.next2_birth_year,
        t.interest
    FROM with_windows s
    LATERAL VIEW EXPLODE(s.interests) t AS interest
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT listener_id, MAX(played_at) AS last_played_at
    FROM plays
    GROUP BY listener_id
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.last_played_at
    FROM exploded s
    LEFT JOIN play_stats p ON p.listener_id = s.id
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER w_interest_by_birth_year_max_1 AS birth_year_max_1_prior_dense_rank,
        LEAD(next2_birth_year, 2) OVER w_interest_by_birth_year_max_1 AS next2_next2_birth_year,
        COUNT(*) OVER (PARTITION BY interest, TO_DATE(last_played_at)) AS rows_in_interest_last_played_at_to_date
    FROM with_stats
    WINDOW w_interest_by_birth_year_max_1 AS (PARTITION BY interest ORDER BY birth_year_max_1_prior DESC, id, interest)
)
-- Subtotals with GROUPING SETS
SELECT
    COALESCE(interest, 'ALL') AS interest,
    birth_year_max_1_prior_dense_rank,
    GROUPING_ID() AS grouping_level,
    ROUND(SUM(birth_year) / NULLIF(SUM(rows_in_interest_last_played_at_to_date), 0), 3) AS birth_year_to_rows_in_interest_last_played_at_to,
    MAX(signup_date) AS last_signup_date,
    SUM(birth_year_max_1_prior) FILTER (WHERE LENGTH(TRIM(username)) >= 7) AS birth_year_max_1_prior_length_trim_username_7
FROM sequenced
GROUP BY GROUPING SETS ((interest, birth_year_max_1_prior_dense_rank), (interest), ())
ORDER BY interest NULLS LAST, birth_year_max_1_prior_dense_rank NULLS LAST
