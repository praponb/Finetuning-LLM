WITH matched AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.listener_id,
        a.played_at,
        COUNT(b.id) AS prior_rows_7d,
        SUM(b.ms_played) AS prior_ms_played_7d
    FROM plays a
    LEFT JOIN plays b
      ON b.listener_id = a.listener_id
     AND b.played_at < a.played_at
     AND b.played_at >= a.played_at - INTERVAL 7 DAYS
    GROUP BY a.id, a.listener_id, a.played_at
),
summary AS (
    -- Summarise per group
    SELECT
        prior_rows_7d,
        listener_id,
        COUNT(*) AS row_count,
        MAX(prior_ms_played_7d) AS max_prior_ms_played_7d,
        COUNT(DISTINCT id) AS distinct_ids,
        MIN(played_at) AS first_played_at,
        MAX(played_at) AS last_played_at
    FROM matched
    WHERE prior_rows_7d > 1
    GROUP BY prior_rows_7d, listener_id
),
with_children_plays AS (
    -- One row per play
    SELECT
        s.prior_rows_7d,
        s.listener_id,
        s.last_played_at,
        p.id AS play_id,
        p.played_at AS play_played_at,
        p.device AS play_device
    FROM summary s
    JOIN plays p ON p.listener_id = s.listener_id AND p.device IN ('desktop', 'car')
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(play_device), ' ', '_')) AS play_device_slug,
        TO_DATE(last_played_at) AS last_played_at_date,
        DATE_TRUNC('YEAR', play_played_at) AS play_played_at_year
    FROM with_children_plays
)
-- Final projection
SELECT
    prior_rows_7d,
    listener_id,
    last_played_at,
    play_played_at,
    play_device,
    last_played_at_date,
    play_played_at_year,
    DATE_TRUNC('WEEK', last_played_at) AS last_played_at_week,
    LOWER(REGEXP_REPLACE(TRIM(play_device_slug), ' ', '_')) AS play_device_slug_slug
FROM enriched
ORDER BY play_played_at_year DESC NULLS FIRST, play_device DESC
