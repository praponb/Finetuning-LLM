WITH cleaned_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        source
    FROM plays
    WHERE track_id >= 15
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        listener_id,
        track_id,
        device,
        source,
        DATE_TRUNC('MONTH', played_at) AS played_at_month,
        played_at + INTERVAL 1 DAYS AS played_at_plus_1d,
        YEAR(played_at) AS played_at_year
    FROM cleaned_plays
    WHERE source = 'radio'
)
-- Subtotals with CUBE
SELECT
    track_id,
    source,
    GROUPING_ID() AS gid,
    ROUND(AVG(played_at_year), 2) AS avg_played_at_year,
    MIN(played_at_year) AS min_played_at_year
FROM scored
GROUP BY CUBE(track_id, source)
ORDER BY track_id NULLS LAST, source NULLS LAST
