WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        TO_DATE(played_at) AS played_at_date
    FROM plays
    WHERE is_skipped = TRUE
      AND played_at >= TIMESTAMP '2025-01-31 10:00:00'
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        played_at,
        ms_played,
        is_skipped,
        played_at_date,
        DATE_FORMAT(played_at_date, 'yyyy-MM-dd') AS played_at_date_label
    FROM scoped
    WHERE played_at >= TIMESTAMP '2025-01-19 11:00:00'
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY YEAR(played_at_date)), 0), 4) AS ms_played_share_of_played_at_date_year,
        ms_played - LAG(ms_played) OVER (PARTITION BY TO_DATE(played_at), is_skipped ORDER BY played_at DESC, id) AS ms_played_change,
        ROW_NUMBER() OVER w_played_at_to_date_is_skipped AS rn,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY TO_DATE(played_at), is_skipped ORDER BY played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        SUM(ms_played) OVER (PARTITION BY TO_DATE(played_at), is_skipped ORDER BY played_at DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS ms_played_sum_2,
        COUNT(*) OVER (PARTITION BY YEAR(played_at_date)) AS rows_in_played_at_date_year
    FROM scored
    WINDOW w_played_at_to_date_is_skipped AS (PARTITION BY TO_DATE(played_at), is_skipped ORDER BY played_at DESC, id)
),
filtered AS (
    -- Compare against the overall min
    SELECT *
    FROM metrics
    WHERE ms_played_share_of_played_at_date_year > (SELECT MIN(ms_played_share_of_played_at_date_year) FROM metrics)
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAG(ms_played_change, 3) OVER w_all_by_played_at AS prev3_ms_played_change,
        NTH_VALUE(played_at, 2) OVER (ORDER BY played_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at_2
    FROM filtered
    WINDOW w_all_by_played_at AS (ORDER BY played_at NULLS LAST, id)
)
SELECT
    id,
    played_at,
    ms_played,
    played_at_date,
    played_at_date_label,
    ms_played_share_of_played_at_date_year,
    ms_played_change,
    rn,
    second_played_at,
    ms_played_sum_2,
    rows_in_played_at_date_year,
    prev3_ms_played_change,
    second_played_at_2
FROM with_windows
WHERE ms_played <= 257789
ORDER BY played_at_date_label DESC, ms_played_share_of_played_at_date_year NULLS LAST, id
LIMIT 20
