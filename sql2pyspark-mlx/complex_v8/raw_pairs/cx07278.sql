WITH valid AS (
    -- Filter plays
    SELECT *
    FROM plays
    WHERE ms_played < 340114
),
unique_rows AS (
    -- Keep the latest row per track_id, listener_id
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY track_id, listener_id ORDER BY ms_played DESC, id DESC) AS rn
        FROM valid
    ) ranked
    WHERE rn = 1
),
filtered AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM unique_rows s
    WHERE s.listener_id IN (
            SELECT p.listener_id
            FROM plays p
            WHERE p.source IS NOT NULL
              AND p.ms_played BETWEEN 112786 AND 225733
        )
),
joined AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.source,
        l.signup_date AS listener_signup_date,
        l.birth_year AS listener_birth_year,
        l.country AS listener_country
    FROM filtered s
    JOIN listeners l ON l.id = s.listener_id
)
SELECT id, metric_name, reading
FROM (
    SELECT id, ms_played, listener_birth_year
    FROM joined
) src
UNPIVOT INCLUDE NULLS (
    reading FOR metric_name IN (ms_played AS ms_played_v, listener_birth_year AS m_listener_birth_year)
) unp
