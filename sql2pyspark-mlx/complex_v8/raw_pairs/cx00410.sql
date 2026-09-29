WITH scoped_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
    WHERE device IN ('desktop', 'tv', 'speaker')
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        COUNT(*) OVER (PARTITION BY YEAR(played_at)) AS rows_in_played_at_year,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY source, device ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        ROUND(ms_played - AVG(ms_played) OVER w_played_at_year, 2) AS ms_played_vs_played_at_year_avg,
        RANK() OVER (PARTITION BY listener_id ORDER BY played_at ASC) AS played_at_rank,
        ROW_NUMBER() OVER w_device_source_by_played_at AS seq_no,
        SIZE(COLLECT_SET(listener_id) OVER (PARTITION BY YEAR(played_at))) AS distinct_listener_id_in_played_at_year,
        MAX(ms_played) OVER (PARTITION BY YEAR(played_at)) AS played_at_year_max_ms_played,
        FIRST_VALUE(played_at) OVER (PARTITION BY device, source ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_played_at,
        FIRST_VALUE(played_at) IGNORE NULLS OVER (PARTITION BY listener_id ORDER BY played_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_played_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY listener_id ORDER BY played_at ASC, id), 3) AS played_at_percent_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY listener_id ORDER BY played_at ASC), 3) AS played_at_percent_rank_2
    FROM scoped_plays
    WINDOW w_played_at_year AS (PARTITION BY YEAR(played_at)), w_device_source_by_played_at AS (PARTITION BY device, source ORDER BY played_at, id)
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(played_at) IGNORE NULLS OVER (PARTITION BY seq_no ORDER BY rows_in_played_at_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_played_at_2,
        LAG(rows_in_played_at_year, 1, 0) OVER (PARTITION BY distinct_listener_id_in_played_at_year ORDER BY first_played_at, id) AS prev_rows_in_played_at_year,
        ROUND(PERCENT_RANK() OVER w_seq_no_by_rows_in_played_at_2, 3) AS rows_in_played_at_year_percent_rank
    FROM windowed
    WINDOW w_seq_no_by_rows_in_played_at_2 AS (PARTITION BY seq_no ORDER BY rows_in_played_at_year)
)
SELECT
    id,
    played_at,
    ms_played,
    source,
    rows_in_played_at_year,
    ms_played_vs_played_at_year_avg,
    seq_no,
    played_at_year_max_ms_played,
    first_played_at,
    first_known_played_at,
    played_at_percent_rank,
    first_known_played_at_2,
    rows_in_played_at_year_percent_rank
FROM sequenced
ORDER BY ms_played_vs_played_at_year_avg DESC
