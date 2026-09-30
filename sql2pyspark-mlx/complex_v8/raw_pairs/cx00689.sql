WITH recent_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status,
        DATEDIFF(DATE '2026-01-01', start_date) AS days_since_start_date,
        ABS(premium - 3465.4) AS premium_distance,
        CASE WHEN premium >= 3465.4 THEN 'HIGH' WHEN premium >= 1132.1 THEN 'MEDIUM' ELSE 'LOW' END AS premium_tier
    FROM policies
    WHERE YEAR(end_date) = 2025
      AND status = 'ACTIVE'
),
unique_rows AS (
    -- Keep the latest row per holder_id
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status,
        days_since_start_date,
        premium_distance,
        premium_tier
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY holder_id ORDER BY premium_distance DESC, id DESC) AS row_num
        FROM recent_policys
    ) ranked
    WHERE row_num = 1
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        LEAST(premium_distance, days_since_start_date) AS least_premium_distance_days_since_start_date
    FROM unique_rows
    WHERE start_date BETWEEN DATE '2024-10-22' AND DATE '2025-03-24'
),
per_group AS (
    -- Summarise per group
    SELECT
        product,
        status,
        DATE_FORMAT(end_date, 'yyyy-MM') AS end_date_ym,
        PERCENTILE_APPROX(premium_distance, 0.5) AS median_premium_distance,
        COUNT(DISTINCT holder_id) AS distinct_holder_ids,
        ARRAY_SORT(COLLECT_SET(status)) AS status_list,
        MIN(least_premium_distance_days_since_start_date) AS min_least_premium_distance_days_since_start_date
    FROM tagged
    GROUP BY product, status, DATE_FORMAT(end_date, 'yyyy-MM')
),
labeled AS (
    -- Add calculated columns
    SELECT
        product,
        status,
        end_date_ym,
        median_premium_distance,
        distinct_holder_ids,
        status_list,
        ROUND(median_premium_distance / NULLIF(distinct_holder_ids, 0), 3) AS median_premium_distance_per_distinct_holder_ids
    FROM per_group
)
-- Final projection
SELECT
    product,
    status,
    end_date_ym,
    distinct_holder_ids,
    status_list,
    median_premium_distance_per_distinct_holder_ids
FROM labeled
WHERE distinct_holder_ids >= 3
ORDER BY product DESC NULLS FIRST, end_date_ym DESC NULLS FIRST
