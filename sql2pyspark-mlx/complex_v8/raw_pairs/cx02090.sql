WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        TO_DATE(played_at) AS played_at_date
    FROM plays
    WHERE source NOT IN ('radio', 'album')
      AND NOT is_skipped
),
joined_listener AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.source,
        s.played_at_date,
        l.plan AS listener_plan,
        l.signup_date AS listener_signup_date
    FROM valid s
    JOIN listeners l ON l.id = s.listener_id
    WHERE l.signup_date >= DATE '2024-08-02'
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY TO_DATE(played_at), listener_plan ORDER BY listener_signup_date DESC) AS listener_signup_date_dense_rank,
        NTILE(4) OVER (ORDER BY listener_signup_date, id) AS listener_signup_date_quartile,
        ROUND(PERCENT_RANK() OVER (ORDER BY listener_signup_date), 3) AS listener_signup_date_percent_rank,
        RANK() OVER (PARTITION BY YEAR(played_at_date), device ORDER BY listener_signup_date DESC NULLS FIRST) AS listener_signup_date_rank,
        RANK() OVER (PARTITION BY TO_DATE(played_at), listener_plan ORDER BY listener_signup_date DESC) AS listener_signup_date_rank_2,
        NTH_VALUE(listener_signup_date, 2) OVER (ORDER BY listener_signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_listener_signup_date,
        RANK() OVER (ORDER BY listener_signup_date) AS listener_signup_date_rank_3,
        DENSE_RANK() OVER (ORDER BY listener_signup_date) AS listener_signup_date_dense_rank_2,
        FIRST_VALUE(played_at) IGNORE NULLS OVER (PARTITION BY YEAR(played_at_date), device ORDER BY listener_signup_date DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_played_at,
        DENSE_RANK() OVER (PARTITION BY YEAR(played_at_date), device ORDER BY listener_signup_date DESC NULLS FIRST, id) AS listener_signup_date_dense_rank_3,
        RANK() OVER w_all_by_listener_signup_date AS listener_signup_date_rank_5,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY YEAR(played_at) ORDER BY played_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        LAST_VALUE(played_at_date) OVER (ORDER BY listener_signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_played_at_date
    FROM joined_listener
    WINDOW w_all_by_listener_signup_date AS (ORDER BY listener_signup_date, id)
)
-- Final projection
SELECT
    id,
    listener_id,
    played_at,
    listener_plan,
    listener_signup_date,
    listener_signup_date_dense_rank,
    listener_signup_date_percent_rank,
    listener_signup_date_rank,
    listener_signup_date_rank_2,
    second_listener_signup_date,
    listener_signup_date_dense_rank_2,
    first_known_played_at,
    listener_signup_date_dense_rank_3,
    second_played_at
FROM windowed
WHERE last_played_at_date IS NOT NULL
ORDER BY listener_signup_date_dense_rank DESC NULLS FIRST
