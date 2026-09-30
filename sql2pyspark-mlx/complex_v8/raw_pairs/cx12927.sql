WITH filtered_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        source
    FROM plays
    WHERE ms_played >= 56858
      AND is_skipped = FALSE
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        source,
        LEAD(played_at) OVER (PARTITION BY listener_id ORDER BY ms_played, id) AS next_played_at,
        COUNT(*) OVER (PARTITION BY source) AS rows_in_source,
        MIN(ms_played) OVER (PARTITION BY listener_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_ms_played_so_far,
        LAG(played_at) OVER (PARTITION BY listener_id ORDER BY played_at, id) AS prev_played_at,
        ROW_NUMBER() OVER (PARTITION BY listener_id ORDER BY ms_played, id) AS ms_played_seq
    FROM filtered_plays
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(min_ms_played_so_far / NULLIF(SUM(min_ms_played_so_far) OVER (PARTITION BY TO_DATE(played_at)), 0), 4) AS min_ms_played_so_far_share_of_played_at_to_date,
        NTILE(3) OVER (ORDER BY ms_played_seq, id) AS ms_played_seq_tercile,
        RANK() OVER w_all_by_ms_played_seq_ties AS ms_played_seq_rank,
        DENSE_RANK() OVER (ORDER BY ms_played_seq) AS ms_played_seq_dense_rank,
        SUM(min_ms_played_so_far) OVER (PARTITION BY TO_DATE(played_at)) AS played_at_to_date_sum_min_ms_played_so_far
    FROM ranked
    WINDOW w_all_by_ms_played_seq_ties AS (ORDER BY ms_played_seq)
),
combined_details AS (
    -- Attach track attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.source,
        s.next_played_at,
        s.min_ms_played_so_far,
        s.prev_played_at,
        s.ms_played_seq_rank,
        s.ms_played_seq_dense_rank,
        s.played_at_to_date_sum_min_ms_played_so_far,
        t.is_explicit AS track_is_explicit,
        a.country AS artist_country,
        a.debut_year AS artist_debut_year
    FROM with_windows s
    JOIN tracks t ON t.id = s.track_id
    JOIN artists a ON a.id = t.artist_id
)
SELECT
    id,
    track_id,
    played_at,
    source,
    next_played_at,
    prev_played_at,
    ms_played_seq_dense_rank,
    played_at_to_date_sum_min_ms_played_so_far,
    track_is_explicit,
    artist_country,
    played_at + INTERVAL 7 DAYS AS played_at_plus_7d,
    CASE WHEN artist_country IN ('KR', 'JP') THEN 'group_a' WHEN artist_country = 'TH' THEN 'group_b' ELSE 'other' END AS artist_country_group
FROM combined_details
