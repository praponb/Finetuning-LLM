WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        LOWER(REGEXP_REPLACE(TRIM(tariff), ' ', '_')) AS tariff_slug
    FROM tariff_windows
    WHERE TO_DATE(valid_from) < DATE '2025-02-18'
),
annotated AS (
    -- Label and score rows
    SELECT *, CONCAT(tariff, '#', CAST(id AS STRING)) AS tariff_ref
    FROM filtered
    WHERE valid_to < TIMESTAMP '2025-04-05 02:00:00'
),
latest_per_key AS (
    -- Keep the latest row per tariff
    SELECT
        id,
        tariff,
        valid_from,
        valid_to,
        tariff_slug,
        tariff_ref
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY tariff ORDER BY valid_from DESC, id DESC) AS recency_rank
        FROM annotated
    ) ranked
    WHERE recency_rank = 1
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        FIRST_VALUE(valid_to) OVER (PARTITION BY tariff ORDER BY valid_from DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_valid_to,
        ROUND(PERCENT_RANK() OVER (PARTITION BY tariff ORDER BY valid_from DESC), 3) AS valid_from_percent_rank
    FROM latest_per_key
)
-- Final projection
SELECT
    id,
    valid_from,
    valid_to,
    tariff_slug,
    first_valid_to,
    valid_from_percent_rank,
    DATE_FORMAT(first_valid_to, 'yyyy-MM-dd HH:00') AS first_valid_to_label,
    ABS(valid_from_percent_rank - 0.3) AS valid_from_percent_rank_distance
FROM windowed
ORDER BY valid_from, id
LIMIT 5
