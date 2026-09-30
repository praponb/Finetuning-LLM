WITH filtered AS (
    -- Scope the listeners rows
    SELECT
        id,
        username,
        country,
        birth_year
    FROM listeners
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        username,
        country,
        birth_year,
        DENSE_RANK() OVER w_all_by_birth_year_ties AS birth_year_dense_rank,
        ROW_NUMBER() OVER (ORDER BY birth_year DESC, id) AS seq_no,
        NTILE(4) OVER (ORDER BY birth_year DESC, id) AS birth_year_quartile,
        ROUND(birth_year / NULLIF(SUM(birth_year) OVER (PARTITION BY country), 0), 4) AS birth_year_share_of_country
    FROM filtered
    WINDOW w_all_by_birth_year_ties AS (ORDER BY birth_year DESC), w_all_by_birth_year AS (ORDER BY birth_year DESC, id)
),
exploded_detail AS (
    -- One row per play
    SELECT
        s.id,
        s.username,
        s.country,
        s.birth_year_dense_rank,
        s.birth_year_quartile,
        s.birth_year_share_of_country,
        p.id AS play_id,
        p.ms_played AS play_ms_played,
        p.played_at AS play_played_at,
        p.source AS play_source,
        p.track_id AS play_track_id
    FROM ranked s
    JOIN plays p ON p.listener_id = s.id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        username,
        country,
        birth_year_share_of_country,
        play_id,
        play_played_at,
        play_source,
        play_track_id,
        ROUND(play_ms_played / NULLIF(SUM(play_ms_played) OVER (PARTITION BY play_source), 0), 4) AS play_ms_played_share_of_play_source,
        birth_year_quartile - LAG(birth_year_quartile) OVER w_play_track_id_by_birth_year AS birth_year_quartile_change,
        LEAD(birth_year_quartile, 3) OVER w_all_by_birth_year_quartile AS next3_birth_year_quartile,
        LEAD(birth_year_quartile, 2) OVER (ORDER BY birth_year_dense_rank DESC, id, play_id) AS next2_birth_year_quartile
    FROM exploded_detail
    WINDOW w_play_track_id_by_birth_year AS (PARTITION BY play_track_id ORDER BY birth_year_dense_rank DESC NULLS FIRST, id, play_id), w_all_by_birth_year_quartile AS (ORDER BY birth_year_quartile ASC NULLS LAST, id, play_id)
),
play_track_id_stats AS (
    -- Avg birth_year_quartile_change per play_track_id
    SELECT play_track_id, AVG(birth_year_quartile_change) AS avg_birth_year_quartile_change
    FROM with_windows
    GROUP BY play_track_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_birth_year_quartile_change,
        ROUND(s.birth_year_quartile_change - g.avg_birth_year_quartile_change, 2) AS birth_year_quartile_change_minus_avg
    FROM with_windows s
    JOIN play_track_id_stats g ON g.play_track_id = s.play_track_id
    WHERE s.birth_year_quartile_change > g.avg_birth_year_quartile_change
)
SELECT id, play_id, measure, metric_value
FROM (
    SELECT id, play_id, play_ms_played_share_of_play_source, birth_year_quartile_change_minus_avg, avg_birth_year_quartile_change, birth_year_share_of_country
    FROM outliers
) src
UNPIVOT (
    metric_value FOR measure IN (play_ms_played_share_of_play_source, birth_year_quartile_change_minus_avg, avg_birth_year_quartile_change, birth_year_share_of_country)
) unp
ORDER BY id, play_id, measure
