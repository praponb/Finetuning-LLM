WITH base_listeners AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        username,
        country,
        signup_date,
        interests
    FROM listeners
    WHERE birth_year >= 1969
      AND plan <> 'family'
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        country,
        signup_date,
        interests,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY signup_date DESC, id) AS signup_date_seq,
        RANK() OVER (PARTITION BY country ORDER BY signup_date DESC, id) AS signup_date_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY country ORDER BY signup_date DESC, id), 3) AS signup_date_percent_rank,
        NTILE(4) OVER (PARTITION BY country ORDER BY signup_date NULLS LAST, id) AS signup_date_quartile,
        NTH_VALUE(signup_date, 2) OVER (PARTITION BY country ORDER BY signup_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_signup_date
    FROM base_listeners
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY signup_date_rank, signup_date_quartile ORDER BY signup_date_quartile ASC), 3) AS signup_date_quartile_cume_dist,
        ROUND(PERCENT_RANK() OVER (PARTITION BY MONTH(second_signup_date) ORDER BY signup_date DESC), 3) AS signup_date_percent_rank_2,
        LEAD(signup_date) OVER (PARTITION BY signup_date_rank, signup_date_quartile ORDER BY signup_date_quartile ASC, id) AS next_signup_date,
        COUNT(*) OVER (PARTITION BY MONTH(second_signup_date)) AS rows_in_second_signup_date_month,
        LAG(signup_date_quartile) OVER w_signup_date_rank_signup_date_2 AS prev_signup_date_quartile
    FROM windowed
    WINDOW w_signup_date_rank_signup_date_2 AS (PARTITION BY signup_date_rank, signup_date_quartile ORDER BY signup_date_quartile ASC, id)
),
country_stats AS (
    -- Max prev_signup_date_quartile per country
    SELECT country, MAX(prev_signup_date_quartile) AS max_prev_signup_date_quartile
    FROM with_windows
    GROUP BY country
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_prev_signup_date_quartile,
        ROUND(s.prev_signup_date_quartile - g.max_prev_signup_date_quartile, 2) AS prev_signup_date_quartile_minus_max
    FROM with_windows s
    JOIN country_stats g ON g.country = s.country
    WHERE s.prev_signup_date_quartile = g.max_prev_signup_date_quartile
),
detail_rows_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.signup_date,
        s.interests,
        s.signup_date_seq,
        s.signup_date_rank,
        s.signup_date_percent_rank,
        s.second_signup_date,
        s.signup_date_percent_rank_2,
        s.next_signup_date,
        s.prev_signup_date_quartile,
        s.prev_signup_date_quartile_minus_max,
        p.id AS play_id,
        p.played_at AS play_played_at,
        p.ms_played AS play_ms_played
    FROM vs_group s
    JOIN plays p ON p.listener_id = s.id AND p.ms_played >= 120663
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        YEAR(next_signup_date) AS next_signup_date_year,
        ABS(play_ms_played - 214343) AS play_ms_played_distance,
        DATEDIFF(DATE '2025-06-30', next_signup_date) AS days_since_next_signup_date
    FROM detail_rows_plays
    WHERE prev_signup_date_quartile_minus_max >= -6.0
)
SELECT
    id,
    interests,
    signup_date_seq,
    signup_date_rank,
    signup_date_percent_rank,
    signup_date_percent_rank_2,
    next_signup_date,
    prev_signup_date_quartile,
    prev_signup_date_quartile_minus_max,
    play_id,
    play_ms_played,
    play_ms_played_distance,
    days_since_next_signup_date,
    COALESCE(signup_date_percent_rank, 0.0) AS signup_date_percent_rank_filled,
    COALESCE(signup_date_rank, 0) AS signup_date_rank_filled
FROM enriched
ORDER BY signup_date_rank_filled DESC, signup_date_percent_rank_filled DESC
