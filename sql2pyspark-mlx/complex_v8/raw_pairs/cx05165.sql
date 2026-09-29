WITH filtered_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug
    FROM policies
    WHERE start_date BETWEEN DATE '2024-03-27' AND DATE '2025-05-02'
      AND holder_id >= 4
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        premium,
        start_date,
        status,
        status_slug,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug_2
    FROM filtered_policys
),
unique_rows AS (
    -- Keep the latest row per status
    SELECT
        id,
        premium,
        start_date,
        status,
        status_slug,
        status_slug_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY status ORDER BY start_date DESC, id DESC) AS rn
        FROM tagged
    ) ranked
    WHERE rn = 1
),
labeled AS (
    -- Derive helper columns
    SELECT *, QUARTER(start_date) AS start_date_quarter
    FROM unique_rows
    WHERE premium BETWEEN 1051.9 AND 4417.0
)
SELECT
    id,
    premium,
    status,
    status_slug,
    start_date_quarter,
    DATEDIFF(DATE '2026-01-01', start_date) AS days_since_start_date
FROM labeled
ORDER BY status, status_slug
