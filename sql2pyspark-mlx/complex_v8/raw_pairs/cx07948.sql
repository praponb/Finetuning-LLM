WITH valid_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm
    FROM tracks
    WHERE bpm < 147
      AND ARRAY_CONTAINS(moods, 'party')
),
with_parent AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.bpm,
        a.debut_year AS artist_debut_year,
        a.country AS artist_country
    FROM valid_tracks s
    JOIN artists a ON a.id = s.artist_id
),
with_details_artist AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.artist_debut_year,
        a.artist_name
    FROM with_parent s
    JOIN artists a ON a.id = s.artist_id
    WHERE UPPER(TRIM(a.artist_name)) LIKE 'K%'
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        artist_id,
        LAG(artist_debut_year) OVER w_artist_id_by_artist_debut_ye AS prev_artist_debut_year,
        RANK() OVER (PARTITION BY artist_id ORDER BY artist_debut_year, id) AS artist_debut_year_rank,
        ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY artist_debut_year, id) AS artist_debut_year_seq,
        LEAD(artist_debut_year, 1, 0) OVER w_artist_id_by_artist_debut_ye AS next_artist_debut_year,
        NTH_VALUE(artist_debut_year, 2) OVER (PARTITION BY artist_id ORDER BY artist_debut_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_artist_debut_year,
        artist_debut_year - LAG(artist_debut_year) OVER w_artist_id_by_artist_debut_ye AS artist_debut_year_change
    FROM with_details_artist
    WINDOW w_artist_id_by_artist_debut_ye AS (PARTITION BY artist_id ORDER BY artist_debut_year, id)
)
-- Subtotals with CUBE
SELECT
    artist_debut_year_seq,
    artist_debut_year_rank,
    GROUPING(artist_debut_year_seq) AS is_total_artist_debut_year_seq,
    COUNT(*) AS cnt
FROM sequenced
GROUP BY CUBE(artist_debut_year_seq, artist_debut_year_rank)
ORDER BY artist_debut_year_seq NULLS LAST, artist_debut_year_rank NULLS LAST
