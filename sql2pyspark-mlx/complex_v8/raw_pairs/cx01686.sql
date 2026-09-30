WITH prepared AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        ROUND(bpm / NULLIF(duration_sec, 0), 3) AS bpm_per_duration_sec,
        LEAST(bpm, duration_sec) AS least_bpm_duration_sec,
        duration_sec - bpm AS duration_sec_minus_bpm
    FROM tracks
    WHERE bpm BETWEEN 89 AND 180
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        title,
        duration_sec,
        is_explicit,
        moods,
        least_bpm_duration_sec,
        duration_sec_minus_bpm,
        NTH_VALUE(bpm_per_duration_sec, 2) OVER (PARTITION BY artist_id ORDER BY least_bpm_duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_bpm_per_duration_sec,
        ROW_NUMBER() OVER w_artist_id_by_least_bpm_durat_2 AS least_bpm_duration_sec_seq
    FROM prepared
    WINDOW w_artist_id_by_least_bpm_durat_2 AS (PARTITION BY artist_id ORDER BY least_bpm_duration_sec, id)
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY is_explicit, least_bpm_duration_sec_seq ORDER BY duration_sec DESC, id) AS duration_sec_decile,
        NTH_VALUE(duration_sec_minus_bpm, 2) OVER (PARTITION BY is_explicit, least_bpm_duration_sec_seq ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_sec_minus_bpm,
        FIRST_VALUE(least_bpm_duration_sec) IGNORE NULLS OVER (PARTITION BY is_explicit, least_bpm_duration_sec_seq ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_least_bpm_duration_sec,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER (PARTITION BY least_bpm_duration_sec_seq, is_explicit), 0), 4) AS duration_sec_share_of_least_bpm_duration_sec_seq
    FROM windowed
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        duration_sec,
        is_explicit,
        moods,
        least_bpm_duration_sec,
        duration_sec_minus_bpm,
        second_bpm_per_duration_sec,
        least_bpm_duration_sec_seq,
        duration_sec_decile,
        duration_sec_share_of_least_bpm_duration_sec_seq,
        LOWER(REGEXP_REPLACE(TRIM(title), ' ', '_')) AS title_slug,
        LENGTH(TRIM(title)) AS title_len
    FROM with_trends
    WHERE NOT is_explicit
)
SELECT id, metric_name, value
FROM (
    SELECT id, least_bpm_duration_sec_seq, least_bpm_duration_sec
    FROM scored
) src
UNPIVOT (
    value FOR metric_name IN (least_bpm_duration_sec_seq AS least_bpm_duration_sec_seq_v, least_bpm_duration_sec AS m_least_bpm_duration_sec)
) unp
ORDER BY id, metric_name
