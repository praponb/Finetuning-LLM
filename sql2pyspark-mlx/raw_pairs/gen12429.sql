WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        INITCAP(TRIM(carrier)) AS carrier_title,
        DATEDIFF(CURRENT_DATE(), shipped_date) / 365.25 AS years_since_shipped_date,
        COALESCE(carrier, 'NONE') AS carrier_clean
    FROM shipments
    WHERE shipped_date >= DATE '2023-05-28'
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY years_since_shipped_date DESC NULLS FIRST, id ASC) AS latest_rank
    FROM cleaned
    QUALIFY latest_rank = 1
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        carrier_clean,
        latest_rank
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY cost DESC, id ASC) AS dedup_rank
        FROM ranked
    ) x
    WHERE dedup_rank = 1
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(latest_rank * 1.15, 2) AS latest_rank_scaled,
        CAST(latest_rank AS DOUBLE) AS latest_rank_num
    FROM latest
    WHERE latest_rank != 1
),
base AS (
    -- Keep recent, valid rows
    SELECT *, LEAST(latest_rank_num, latest_rank_scaled) AS least_latest_rank_num_latest_rank_scaled
    FROM valid
    WHERE latest_rank_num BETWEEN 45 AND 61
)
-- Multi-level aggregation
SELECT
    CASE WHEN least_latest_rank_num_latest_rank_scaled >= 86 THEN 'HIGH' ELSE 'LOW' END AS least_latest_rank_num_latest_rank_scaled_tier,
    ROUND(AVG(latest_rank_scaled), 2) AS avg_latest_rank_scaled,
    APPROX_PERCENTILE(least_latest_rank_num_latest_rank_scaled, ARRAY(0.25, 0.5, 0.75)) AS least_latest_rank_num_latest_rank_scaled_quartiles
FROM base
GROUP BY 1;
