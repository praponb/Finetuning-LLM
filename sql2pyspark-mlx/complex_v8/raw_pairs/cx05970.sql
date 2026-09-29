WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        is_explicit,
        moods,
        DATE_FORMAT(release_date, 'yyyy-MM-dd') AS release_date_label
    FROM tracks
    WHERE artist_id IS NOT NULL
      AND ARRAY_CONTAINS(moods, 'party')
),
with_counts AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE t.is_explicit = TRUE) FROM tracks t WHERE t.artist_id = s.artist_id) AS track_is_explicit_count,
        (SELECT COUNT(*) FROM tracks t WHERE t.artist_id = s.artist_id) AS track_count
    FROM valid s
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        artist_id,
        title,
        is_explicit,
        moods,
        track_is_explicit_count,
        track_count,
        ROUND(track_is_explicit_count / NULLIF(SUM(track_is_explicit_count) OVER w_is_explicit, 0), 4) AS track_is_explicit_count_share_of_is_explicit,
        LAG(track_count, 3) OVER (PARTITION BY is_explicit, track_count ORDER BY track_is_explicit_count, id) AS prev3_track_count,
        ROUND(track_count / NULLIF(SUM(track_count) OVER (PARTITION BY is_explicit), 0), 4) AS track_count_share_of_is_explicit,
        LAG(track_count, 2) OVER (PARTITION BY artist_id ORDER BY track_count DESC, id) AS prev2_track_count,
        track_count - LAG(track_count) OVER w_artist_id_by_track_count AS track_count_change,
        LEAD(track_is_explicit_count, 2) OVER w_is_explicit_track_count_by_t AS next2_track_is_explicit_count
    FROM with_counts
    WINDOW w_is_explicit AS (PARTITION BY is_explicit), w_artist_id_by_track_count AS (PARTITION BY artist_id ORDER BY track_count DESC, id), w_is_explicit_track_count_by_t AS (PARTITION BY is_explicit, track_count ORDER BY track_is_explicit_count, id)
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        artist_id,
        title,
        moods,
        track_count,
        prev3_track_count,
        track_count_share_of_is_explicit,
        prev2_track_count,
        next2_track_is_explicit_count,
        ROUND(track_count_change - AVG(track_count_change) OVER w_artist_id_track_is_explicit, 2) AS track_count_change_vs_artist_id_track_is_explici,
        track_count_change - LAG(track_count_change) OVER (PARTITION BY track_is_explicit_count, artist_id ORDER BY track_count, id) AS track_count_change_change,
        MAX(prev2_track_count) OVER (PARTITION BY track_is_explicit_count, artist_id ORDER BY track_count, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_prev2_track_count_so_far,
        ROUND(track_count_change / NULLIF(SUM(track_count_change) OVER (PARTITION BY artist_id, track_is_explicit_count), 0), 4) AS track_count_change_share_of_artist_id_track_is_e,
        ROUND(CUME_DIST() OVER (PARTITION BY track_is_explicit_count, artist_id ORDER BY track_count), 3) AS track_count_cume_dist
    FROM sequenced
    WINDOW w_artist_id_track_is_explicit AS (PARTITION BY artist_id, track_is_explicit_count)
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        title,
        moods,
        track_count,
        prev3_track_count,
        track_count_share_of_is_explicit,
        track_count_change_vs_artist_id_track_is_explici,
        track_count_change_change,
        track_count_cume_dist,
        ROUND(track_count / NULLIF(prev2_track_count, 0), 3) AS track_count_per_prev2_track_count
    FROM windowed
)
SELECT
    id,
    moods,
    track_count,
    prev3_track_count,
    track_count_share_of_is_explicit,
    track_count_change_vs_artist_id_track_is_explici,
    track_count_change_change,
    track_count_cume_dist,
    track_count_per_prev2_track_count,
    CASE WHEN track_count_cume_dist >= 0.6 THEN 'A' WHEN track_count_cume_dist >= 0.2 THEN 'B' ELSE 'C' END AS track_count_cume_dist_tier,
    SIZE(moods) AS moods_count
FROM labeled
WHERE track_count_cume_dist > 0.4
ORDER BY id
LIMIT 20
