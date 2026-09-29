WITH recent_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        QUARTER(start_date) AS start_date_quarter
    FROM policies
    WHERE premium <= 4678.8
      AND start_date >= DATE '2025-01-05'
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        UPPER(TRIM(status)) AS status_clean,
        CONCAT(status, '#', CAST(id AS STRING)) AS status_ref
    FROM recent_policys
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(start_date) IGNORE NULLS OVER (PARTITION BY holder_id, YEAR(start_date) ORDER BY end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_start_date,
        MIN(premium) OVER (PARTITION BY YEAR(end_date)) AS end_date_year_min_premium
    FROM annotated
),
deduped AS (
    -- Keep the latest row per status
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        start_date_quarter,
        status_clean,
        status_ref,
        last_known_start_date,
        end_date_year_min_premium
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY status ORDER BY premium DESC, id DESC) AS rn
        FROM windowed
    ) ranked
    WHERE rn = 1
)
SELECT
    start_date_quarter,
    status,
    COUNT(*) FILTER (WHERE status IS NOT NULL) AS n_status_is_not_null,
    COUNT(*) AS cnt
FROM deduped
GROUP BY ROLLUP(start_date_quarter, status)
ORDER BY start_date_quarter NULLS LAST, status NULLS LAST
