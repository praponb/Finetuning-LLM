WITH valid_listeners AS (
    -- Scope the listeners rows
    SELECT
        id,
        plan,
        country,
        birth_year,
        interests,
        CASE WHEN country IN ('KR', 'BR') THEN 'group_a' WHEN country = 'JP' THEN 'group_b' ELSE 'other' END AS country_group,
        LENGTH(TRIM(username)) AS username_len,
        UPPER(TRIM(country)) AS country_clean
    FROM listeners
    WHERE country IS NOT NULL
      AND signup_date BETWEEN DATE '2024-08-02' AND DATE '2025-10-18'
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        LEAD(birth_year) OVER (ORDER BY username_len ASC NULLS LAST, id) AS next_birth_year,
        NTH_VALUE(username_len, 2) OVER (ORDER BY username_len ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_username_len,
        COUNT(*) OVER (PARTITION BY country) AS rows_in_country,
        LAG(birth_year) OVER w_all_by_username_len AS prev_birth_year,
        ROUND(AVG(birth_year) OVER w_country, 2) AS country_avg_birth_year
    FROM valid_listeners
    WINDOW w_all_by_username_len AS (ORDER BY username_len ASC NULLS LAST, id), w_country AS (PARTITION BY country)
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        plan,
        country,
        birth_year,
        interests,
        country_group,
        next_birth_year,
        second_username_len,
        rows_in_country,
        country_avg_birth_year,
        COALESCE(rows_in_country, 0) AS rows_in_country_filled
    FROM with_windows
    WHERE country_avg_birth_year < 1993.6
),
line_level_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.plan,
        s.birth_year,
        s.interests,
        s.country_avg_birth_year,
        s.rows_in_country_filled,
        p.id AS play_id,
        p.ms_played AS play_ms_played,
        p.track_id AS play_track_id
    FROM labeled s
    LEFT JOIN plays p ON p.listener_id = s.id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, play_id, metric_name, reading
FROM (
    SELECT id, play_id, play_ms_played, birth_year, rows_in_country_filled
    FROM line_level_plays
) src
UNPIVOT (
    reading FOR metric_name IN (play_ms_played AS play_ms_played_v, birth_year AS m_birth_year, rows_in_country_filled AS m_rows_in_country_filled)
) unp
ORDER BY id, play_id, metric_name
