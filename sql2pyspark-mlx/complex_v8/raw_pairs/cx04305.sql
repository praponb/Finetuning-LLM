WITH cleaned AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        LENGTH(TRIM(source)) AS source_len
    FROM plays
    WHERE MONTH(played_at) IN (1, 2)
      AND track_id IS NOT NULL
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY device, listener_id ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        COUNT(*) OVER (PARTITION BY source_len, source) AS rows_in_source_len_source
    FROM cleaned
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM plays p WHERE p.listener_id = s.listener_id) AS play_count,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.listener_id = s.listener_id) AS last_played_at,
        (SELECT COUNT(*) FROM plays p WHERE p.listener_id = s.listener_id) AS play_count_2
    FROM sequenced s
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        is_skipped,
        source,
        second_played_at,
        rows_in_source_len_source,
        play_count,
        NTILE(4) OVER w_played_at_month_by_play_coun AS play_count_2_quartile,
        ROUND(PERCENT_RANK() OVER w_played_at_month_by_play_coun_2, 3) AS play_count_2_percent_rank,
        SUM(source_len) OVER (PARTITION BY device) AS device_sum_source_len
    FROM summarized_rows
    WINDOW w_played_at_month_by_play_coun AS (PARTITION BY MONTH(played_at) ORDER BY play_count_2 DESC, id), w_played_at_month_by_play_coun_2 AS (PARTITION BY MONTH(played_at) ORDER BY play_count_2 DESC)
)
SELECT
    play_count_2_quartile,
    listener_id,
    PERCENTILE_APPROX(play_count_2_percent_rank, 0.5) AS median_play_count_2_percent_rank
FROM with_trends
GROUP BY GROUPING SETS ((play_count_2_quartile, listener_id), (play_count_2_quartile))
ORDER BY play_count_2_quartile NULLS LAST, listener_id NULLS LAST
