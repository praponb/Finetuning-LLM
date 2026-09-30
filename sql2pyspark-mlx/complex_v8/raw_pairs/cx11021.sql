WITH cleaned_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE end_date IS NOT NULL
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        FLOOR(premium / 1000) * 1000 AS premium_bucket,
        LENGTH(TRIM(status)) AS status_len
    FROM cleaned_policys
    WHERE holder_id IS NOT NULL
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_FORMAT(end_date, 'yyyy-MM') AS end_date_ym,
        status,
        ROUND(SUM(premium), 2) AS total_premium,
        COUNT(DISTINCT holder_id) AS distinct_holder_ids,
        ROUND(SUM(status_len) / NULLIF(SUM(premium), 0), 3) AS status_len_to_premium_ratio
    FROM enriched
    WHERE premium < 4417.0
    GROUP BY DATE_FORMAT(end_date, 'yyyy-MM'), status
),
labeled AS (
    -- Derive helper columns
    SELECT *, ABS(distinct_holder_ids - 6) AS distinct_holder_ids_distance
    FROM grouped
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        SUBSTRING(TRIM(status), 1, 4) AS status_prefix,
        ROUND(total_premium / NULLIF(distinct_holder_ids, 0), 3) AS total_premium_per_distinct_holder_ids
    FROM labeled
),
tagged_2 AS (
    -- Label and score rows
    SELECT
        end_date_ym,
        status,
        total_premium,
        status_len_to_premium_ratio,
        distinct_holder_ids_distance,
        status_prefix,
        total_premium_per_distinct_holder_ids,
        LOWER(REGEXP_REPLACE(TRIM(status_prefix), ' ', '_')) AS status_prefix_slug
    FROM tagged
)
-- Final projection
SELECT
    end_date_ym,
    status,
    distinct_holder_ids_distance,
    status_prefix,
    total_premium_per_distinct_holder_ids,
    status_prefix_slug,
    INITCAP(TRIM(status)) AS status_clean
FROM tagged_2
WHERE distinct_holder_ids_distance <= 9
