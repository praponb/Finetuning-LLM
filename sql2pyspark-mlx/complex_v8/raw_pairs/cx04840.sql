WITH valid_plays AS (
    -- Scope the plays rows
    SELECT *
    FROM plays
    WHERE listener_id IS NOT NULL
),
combined_details AS (
    -- Join listeners
    SELECT s.*, l.country AS listener_country
    FROM valid_plays s
    JOIN listeners l ON l.id = s.listener_id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(ms_played, 2) OVER (PARTITION BY listener_country ORDER BY played_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played,
        ROUND(PERCENT_RANK() OVER (PARTITION BY is_skipped, track_id ORDER BY played_at DESC), 3) AS played_at_percent_rank,
        LAST_VALUE(ms_played) IGNORE NULLS OVER (PARTITION BY source ORDER BY played_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ms_played,
        ROUND(AVG(ms_played) OVER (PARTITION BY listener_country ORDER BY played_at DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS ms_played_avg_2_prior,
        COUNT(*) OVER (PARTITION BY listener_country) AS rows_in_listener_country,
        ROW_NUMBER() OVER (PARTITION BY source ORDER BY played_at ASC, id) AS played_at_seq,
        FIRST_VALUE(played_at) OVER (PARTITION BY listener_country ORDER BY played_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_played_at,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY listener_country ORDER BY played_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        SUM(ms_played) OVER (PARTITION BY listener_country ORDER BY played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ms_played,
        SUM(ms_played) OVER (PARTITION BY listener_country) AS listener_country_sum_ms_played
    FROM combined_details
)
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    device,
    is_skipped,
    source,
    second_ms_played,
    played_at_percent_rank,
    ms_played_avg_2_prior,
    rows_in_listener_country,
    played_at_seq,
    second_played_at,
    running_ms_played,
    listener_country_sum_ms_played
FROM ranked
ORDER BY source DESC, played_at_seq DESC NULLS FIRST
