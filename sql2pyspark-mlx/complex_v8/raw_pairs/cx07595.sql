WITH prepared AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE (duration_sec <= 353 OR NOT is_explicit)
),
qualified AS (
    -- Keep rows whose artist matches
    SELECT s.*
    FROM prepared s
    WHERE EXISTS (
            SELECT 1
            FROM artists a
            WHERE a.id = s.artist_id
              AND a.is_verified = FALSE
              AND a.country = 'BR'
        )
),
derived AS (
    -- Derive helper columns
    SELECT
        *,
        MONTH(release_date) AS release_date_month,
        CASE WHEN bpm >= 143 THEN 'A' WHEN bpm >= 104 THEN 'B' ELSE 'C' END AS bpm_tier
    FROM qualified
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LEAD(release_date, 3) OVER w_bpm_tier_by_release_date AS next3_release_date,
        ROUND(release_date_month - AVG(release_date_month) OVER w_release_date_month_is_explic, 2) AS release_date_month_vs_release_date_month_is_expl,
        ROUND(PERCENT_RANK() OVER (PARTITION BY bpm_tier ORDER BY release_date), 3) AS release_date_percent_rank
    FROM derived
    WINDOW w_bpm_tier_by_release_date AS (PARTITION BY bpm_tier ORDER BY release_date, id), w_release_date_month_is_explic AS (PARTITION BY MONTH(release_date), is_explicit)
)
-- Subtotals with CUBE
SELECT
    release_date_month,
    COALESCE(bpm_tier, 'ALL') AS bpm_tier,
    COUNT(*) AS n_rows
FROM sequenced
GROUP BY CUBE(release_date_month, bpm_tier)
