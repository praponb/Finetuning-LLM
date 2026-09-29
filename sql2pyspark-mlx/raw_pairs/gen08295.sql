WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_name,
        opened_date,
        sqft,
        ROUND(sqft, 2) AS sqft_rounded
    FROM stores
    WHERE opened_date > DATE '2023-03-30'
      AND city NOT IN ('London', 'Chiang Mai', 'Singapore')
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        opened_date,
        sqft,
        sqft_rounded,
        CUME_DIST() OVER (ORDER BY sqft) AS sqft_cume_dist,
        PERCENT_RANK() OVER (ORDER BY sqft_rounded DESC) AS sqft_rounded_percent_rank,
        AVG(sqft_rounded) OVER (PARTITION BY YEAR(opened_date) ORDER BY sqft, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS sqft_rounded_avg_5,
        ROW_NUMBER() OVER (PARTITION BY YEAR(opened_date) ORDER BY opened_date DESC, id) AS event_seq
    FROM base
    WHERE LENGTH(store_name) < 12
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        sqft,
        sqft_cume_dist,
        sqft_rounded_percent_rank,
        sqft_rounded_avg_5,
        event_seq,
        LEAD(sqft) OVER (ORDER BY sqft_cume_dist, id) AS next_sqft,
        LAG(sqft) OVER (ORDER BY sqft_cume_dist, id) AS prev_sqft,
        LEAD(sqft_rounded_percent_rank) OVER (ORDER BY sqft_cume_dist, id) AS next_sqft_rounded_percent_rank,
        ROW_NUMBER() OVER (ORDER BY sqft_cume_dist, id) AS row_seq
    FROM with_windows
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM scored
    EXCEPT DISTINCT
    SELECT id
    FROM stores
    WHERE region IS NULL
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN kept_keys k ON s.id = k.id
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM verified
    INTERSECT
    SELECT id
    FROM stores
    WHERE region IN ('East', 'West')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM verified s
    INNER JOIN remaining_keys k ON s.id = k.id
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        sqft_cume_dist,
        sqft_rounded_percent_rank,
        event_seq,
        next_sqft,
        prev_sqft,
        next_sqft_rounded_percent_rank,
        row_seq,
        CASE WHEN event_seq >= 8 THEN 'High' WHEN event_seq >= 4 THEN 'Medium' ELSE 'Low' END AS event_seq_level
    FROM kept_rows
    WHERE sqft_rounded_percent_rank >= 0.79
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN next_sqft >= 15214 THEN 'LARGE' ELSE 'SMALL' END AS next_sqft_tier,
        CASE WHEN sqft_rounded_percent_rank >= 0.76 THEN 'HIGH' ELSE 'LOW' END AS sqft_rounded_percent_rank_tier,
        COUNT(DISTINCT id) AS distinct_ids,
        MAX(sqft_cume_dist) AS max_sqft_cume_dist,
        COUNT(DISTINCT id) AS distinct_ids_2
    FROM cleaned
    GROUP BY GROUPING SETS ((CASE WHEN next_sqft >= 15214 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN sqft_rounded_percent_rank >= 0.76 THEN 'HIGH' ELSE 'LOW' END), (CASE WHEN next_sqft >= 15214 THEN 'LARGE' ELSE 'SMALL' END), ())
)
SELECT
    next_sqft_tier,
    sqft_rounded_percent_rank_tier,
    distinct_ids
FROM grouped
ORDER BY distinct_ids DESC NULLS LAST;
