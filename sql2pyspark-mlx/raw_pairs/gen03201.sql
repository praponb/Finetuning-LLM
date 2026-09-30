WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(sqft, TRUE) OVER (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_sqft,
        SUM(sqft) OVER (ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_sqft
    FROM stores
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date, id) AS dedup_rank
        FROM with_windows
    ) ranked
    WHERE dedup_rank = 1
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft DESC, id) = 1
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_name,
        city,
        last_known_sqft,
        cumulative_sqft,
        COALESCE(sqft, 0) sqft_filled,
        LAST_DAY(opened_date) opened_date_month_end,
        COALESCE(region, 'OTHER') region_filled
    FROM deduped
    WHERE opened_date > DATE '2023-08-17'
      AND region IS DISTINCT FROM 'West'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_name,
        city,
        last_known_sqft,
        cumulative_sqft,
        region_filled,
        NTILE(4) OVER (PARTITION BY city ORDER BY cumulative_sqft DESC, id) cumulative_sqft_bucket,
        SUM(last_known_sqft) OVER (PARTITION BY city ORDER BY opened_date_month_end, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_last_known_sqft,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('YEAR', opened_date_month_end), city ORDER BY sqft_filled DESC, id) row_seq
    FROM filtered
)
SELECT
    id,
    store_name,
    last_known_sqft,
    cumulative_sqft,
    region_filled,
    row_seq,
    CASE WHEN store_name RLIKE '^(Airport Kiosk|Mall Central)$' THEN 'GROUP_A' WHEN store_name RLIKE '^(Siam Flagship|Downtown Express)$' THEN 'GROUP_B' ELSE 'CORE' END AS store_name_zone
FROM scored
ORDER BY cumulative_sqft DESC NULLS LAST;
