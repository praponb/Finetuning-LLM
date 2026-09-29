WITH prepared_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE duration_sec <= 352
),
joined AS (
    -- Join artists
    SELECT
        s.*,
        a.is_verified AS artist_is_verified,
        a.genre AS artist_genre,
        a.artist_name
    FROM prepared_tracks s
    JOIN artists a ON a.id = s.artist_id
    WHERE UPPER(TRIM(a.artist_name)) LIKE 'P%'
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        title,
        release_date,
        is_explicit,
        bpm,
        artist_is_verified,
        LAG(bpm) OVER (PARTITION BY is_explicit ORDER BY bpm ASC, id) AS prev_bpm,
        NTH_VALUE(bpm, 2) OVER (PARTITION BY is_explicit ORDER BY bpm ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_bpm,
        RANK() OVER (PARTITION BY is_explicit ORDER BY bpm ASC) AS bpm_rank,
        ROW_NUMBER() OVER (PARTITION BY is_explicit ORDER BY bpm ASC, id) AS bpm_seq,
        ROUND(duration_sec - AVG(duration_sec) OVER (PARTITION BY artist_id), 2) AS duration_sec_vs_artist_id_avg
    FROM joined
)
SELECT
    id,
    title,
    release_date,
    is_explicit,
    prev_bpm,
    second_bpm,
    bpm_rank,
    bpm_seq
FROM sequenced
