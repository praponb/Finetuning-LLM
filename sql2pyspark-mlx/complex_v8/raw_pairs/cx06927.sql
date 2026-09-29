WITH scoped AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        CASE WHEN source IN ('album', 'playlist') THEN 'group_a' WHEN source = 'search' THEN 'group_b' ELSE 'other' END AS source_group,
        DAYOFWEEK(played_at) AS played_at_dayofweek
    FROM plays
    WHERE track_id IS NOT NULL
      AND ms_played <= 236512
),
with_parent_listener AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.device,
        s.source_group,
        s.played_at_dayofweek,
        l.birth_year AS listener_birth_year,
        l.country AS listener_country,
        t.duration_sec AS track_duration_sec,
        t.bpm AS track_bpm
    FROM scoped s
    JOIN listeners l ON l.id = s.listener_id
    LEFT JOIN tracks t ON t.id = s.track_id
    WHERE l.birth_year > 1966
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(ms_played, 1, 0) OVER (PARTITION BY listener_country, track_id ORDER BY played_at_dayofweek, id) AS next_ms_played,
        NTILE(3) OVER (PARTITION BY listener_country, track_id ORDER BY played_at_dayofweek, id) AS played_at_dayofweek_tercile,
        ROUND(AVG(track_bpm) OVER (PARTITION BY track_id ORDER BY track_duration_sec, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS track_bpm_avg_3,
        LAG(played_at_dayofweek, 2, 0) OVER w_source_group_track_id_by_pla AS prev2_played_at_dayofweek,
        NTH_VALUE(played_at_dayofweek, 2) OVER (PARTITION BY track_id ORDER BY track_duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at_dayofweek,
        FIRST_VALUE(played_at_dayofweek) IGNORE NULLS OVER (PARTITION BY source_group, track_id ORDER BY played_at_dayofweek, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_played_at_dayofweek,
        ROUND(PERCENT_RANK() OVER (PARTITION BY source_group, track_id ORDER BY played_at_dayofweek), 3) AS played_at_dayofweek_percent_rank,
        SUM(ms_played) OVER w_source_group AS source_group_sum_ms_played,
        MIN(listener_birth_year) OVER w_source_group AS source_group_min_listener_birth_year,
        ROUND(played_at_dayofweek - AVG(played_at_dayofweek) OVER (PARTITION BY device), 2) AS played_at_dayofweek_vs_device_avg,
        MIN(ms_played) OVER w_device AS device_min_ms_played,
        MAX(played_at_dayofweek) OVER (PARTITION BY source_group) AS source_group_max_played_at_dayofweek,
        ROUND(listener_birth_year / NULLIF(SUM(listener_birth_year) OVER (PARTITION BY device), 0), 4) AS listener_birth_year_share_of_device,
        ROUND(played_at_dayofweek - AVG(played_at_dayofweek) OVER w_source_group, 2) AS played_at_dayofweek_vs_source_group_avg,
        ROUND(PERCENT_RANK() OVER w_source_group_track_id_by_pla, 3) AS played_at_dayofweek_percent_rank_2,
        ROUND(CUME_DIST() OVER w_listener_country_track_id_by, 3) AS played_at_dayofweek_cume_dist
    FROM with_parent_listener
    WINDOW w_source_group_track_id_by_pla AS (PARTITION BY source_group, track_id ORDER BY played_at_dayofweek, id), w_source_group AS (PARTITION BY source_group), w_device AS (PARTITION BY device), w_listener_country_track_id_by AS (PARTITION BY listener_country, track_id ORDER BY played_at_dayofweek, id)
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(track_bpm_avg_3) OVER (PARTITION BY prev2_played_at_dayofweek, first_known_played_at_dayofweek) AS prev2_played_at_dayofweek_first_known_played_at_,
        DENSE_RANK() OVER w_all_by_listener_birth_year_s AS listener_birth_year_share_of_device_dense_rank,
        RANK() OVER w_played_at_dayofweek_tercile AS played_at_dayofweek_percent_rank_rank,
        ROUND(listener_birth_year / NULLIF(SUM(listener_birth_year) OVER w_source_group, 0), 4) AS listener_birth_year_share_of_source_group,
        FIRST_VALUE(ms_played) OVER (PARTITION BY played_at_dayofweek ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_ms_played,
        ROUND(AVG(track_bpm_avg_3) OVER (PARTITION BY source_group), 2) AS source_group_avg_track_bpm_avg_3,
        NTH_VALUE(track_bpm_avg_3, 2) OVER (PARTITION BY device, played_at_dayofweek ORDER BY listener_birth_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_track_bpm_avg_3
    FROM sequenced
    WINDOW w_all_by_listener_birth_year_s AS (ORDER BY listener_birth_year_share_of_device), w_played_at_dayofweek_tercile AS (PARTITION BY played_at_dayofweek_tercile ORDER BY played_at_dayofweek_percent_rank, id), w_source_group AS (PARTITION BY source_group)
),
totals AS (
    -- Group-level totals
    SELECT
        CASE WHEN listener_birth_year >= 1986 THEN 'large' WHEN listener_birth_year >= 1975 THEN 'medium' ELSE 'small' END AS listener_birth_year_tier,
        track_id,
        ROUND(100 * COUNT(CASE WHEN device = 'speaker' THEN 1 END) / COUNT(*), 1) AS pct_device_speaker,
        COUNT(*) FILTER (WHERE device = 'speaker') AS n_device_speaker,
        MIN(track_bpm_avg_3) AS min_track_bpm_avg_3,
        ARRAY_SORT(COLLECT_SET(listener_country)) AS listener_country_list,
        ROUND(SUM(listener_birth_year_share_of_source_group), 2) AS total_listener_birth_year_share_of_source_group
    FROM with_trends
    GROUP BY 1, 2
),
windowed AS (
    -- Previous / next values per key
    SELECT
        listener_birth_year_tier,
        track_id,
        min_track_bpm_avg_3,
        listener_country_list,
        DENSE_RANK() OVER (PARTITION BY n_device_speaker ORDER BY total_listener_birth_year_share_of_source_group ASC, listener_birth_year_tier, track_id) AS total_listener_birth_year_share_of_source_group_,
        ROUND(pct_device_speaker - AVG(pct_device_speaker) OVER (PARTITION BY listener_birth_year_tier, track_id), 2) AS pct_device_speaker_vs_listener_birth_year_tier_t,
        MIN(pct_device_speaker) OVER (PARTITION BY listener_birth_year_tier, track_id) AS listener_birth_year_tier_track_id_min_pct_device,
        ROUND(min_track_bpm_avg_3 / NULLIF(SUM(min_track_bpm_avg_3) OVER (PARTITION BY listener_birth_year_tier, track_id), 0), 4) AS min_track_bpm_avg_3_share_of_listener_birth_year
    FROM totals
)
-- Final projection
SELECT
    listener_birth_year_tier,
    listener_country_list,
    listener_birth_year_tier_track_id_min_pct_device,
    min_track_bpm_avg_3_share_of_listener_birth_year,
    CASE WHEN min_track_bpm_avg_3 >= 162.0 THEN 'large' WHEN min_track_bpm_avg_3 >= 84.0 THEN 'medium' ELSE 'small' END AS min_track_bpm_avg_3_tier
FROM windowed
ORDER BY min_track_bpm_avg_3_tier
