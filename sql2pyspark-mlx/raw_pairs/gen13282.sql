WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        channel,
        starts_on,
        ends_on,
        discount_pct,
        ROUND(DATEDIFF(starts_on, ends_on) / 30, 1) AS months_ends_on_to_starts_on
    FROM promotions
    WHERE channel IS NOT NULL
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT id, starts_on
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY channel ORDER BY starts_on, id) = 1
),
base_promotions AS (
    -- Keep recent, valid rows
    SELECT *, ADD_MONTHS(starts_on, 3) AS starts_on_plus_months
    FROM ranked
    WHERE starts_on < '2024-02-01'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY YEAR(starts_on) ORDER BY starts_on_plus_months, id) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY MONTH(starts_on) ORDER BY starts_on_plus_months DESC, id) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY YEAR(starts_on) ORDER BY starts_on_plus_months DESC, id) AS event_seq_2
    FROM base_promotions
    WHERE YEAR(starts_on_plus_months) = 2023
)
-- Grouped metrics
SELECT
    DATE_TRUNC('YEAR', starts_on) AS year_start,
    DATE_TRUNC('QUARTER', starts_on_plus_months) AS quarter_start,
    ROUND(AVG(event_seq_2), 2) AS avg_event_seq_2,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(AVG(event_seq), 2) AS avg_event_seq,
    SUM(event_seq_2) AS total_event_seq_2,
    COUNT(DISTINCT id) AS distinct_ids_2,
    COUNT(*) AS num_rows,
    GROUPING_ID() AS gid
FROM scored
GROUP BY GROUPING SETS ((DATE_TRUNC('YEAR', starts_on), DATE_TRUNC('QUARTER', starts_on_plus_months)), (DATE_TRUNC('YEAR', starts_on)))
ORDER BY distinct_ids NULLS LAST;
