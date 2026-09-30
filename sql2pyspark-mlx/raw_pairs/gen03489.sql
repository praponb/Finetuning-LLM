WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY region, city ORDER BY sqft DESC, id) AS dedup_rank
    FROM stores
    QUALIFY dedup_rank = 1
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        dedup_rank,
        LOWER(store_name) AS store_name_lower
    FROM latest
    WHERE opened_date >= TO_DATE('2023-03-07')
      AND region IN ('East', 'West', 'Central')
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY region ORDER BY sqft DESC NULLS FIRST, id) AS row_num
    FROM filtered
    QUALIFY row_num = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        city,
        dedup_rank,
        row_num,
        ROW_NUMBER() OVER (PARTITION BY city, region ORDER BY sqft, id) AS seq_no,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY region), 4) AS sqft_share
    FROM deduped
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        city,
        row_num,
        seq_no
    FROM with_windows
    QUALIFY RANK() OVER (PARTITION BY city ORDER BY row_num DESC) = 1
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, INITCAP(TRIM(city)) AS city_title
    FROM ranked
    WHERE city IN ('Singapore', 'New York')
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY city ORDER BY seq_no, id) AS seq_no_bucket,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY seq_no DESC, id) AS row_seq
    FROM valid
    WHERE seq_no <> 6
)
-- Grouped metrics
SELECT
    seq_no,
    SIZE(COLLECT_SET(city)) AS num_cities,
    COUNT_IF(city = 'Bangkok') AS bangkok_events,
    COUNT(*) FILTER (WHERE city = 'New York') AS new_york_rows
FROM scored
GROUP BY seq_no
ORDER BY num_cities DESC NULLS FIRST
