WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, CONCAT_WS(',', store_name, city) AS store_name_city_key
    FROM stores
    WHERE opened_date >= DATE '2023-03-07'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(sqft) OVER (PARTITION BY region, city ORDER BY sqft ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_sqft,
        MAX(sqft) OVER (PARTITION BY region, city ORDER BY sqft ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_sqft,
        MIN(sqft) OVER (PARTITION BY region, city ORDER BY sqft ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_sqft,
        COUNT(sqft) OVER (PARTITION BY region, city ORDER BY sqft ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_sqft,
        RANK() OVER (PARTITION BY region ORDER BY sqft ASC NULLS FIRST) AS sqft_rank
    FROM valid
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        store_name,
        city,
        region,
        sqft,
        store_name_city_key,
        running_sum_sqft,
        running_min_sqft,
        running_count_sqft,
        sqft_rank
    FROM metrics
    QUALIFY RANK() OVER (PARTITION BY region, city ORDER BY sqft ASC NULLS FIRST) = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY region ORDER BY running_sum_sqft DESC NULLS FIRST) AS running_sum_sqft_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY running_sum_sqft DESC, id ASC) AS row_seq,
        NTILE(10) OVER (PARTITION BY region ORDER BY running_count_sqft DESC, id ASC) AS running_count_sqft_bucket,
        ROUND(running_min_sqft / SUM(running_min_sqft) OVER (PARTITION BY region), 4) AS running_min_sqft_share,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft ASC, id ASC) AS row_seq_2
    FROM ranked
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        region,
        store_name_city_key,
        running_sum_sqft,
        running_min_sqft,
        running_count_sqft_bucket,
        running_min_sqft_share,
        row_seq_2
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY running_min_sqft_share DESC NULLS FIRST, id ASC) = 1
),
scored_stores AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY running_min_sqft ASC, id ASC) AS event_seq,
        AVG(running_min_sqft) OVER (PARTITION BY region ORDER BY running_min_sqft ASC, id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS running_min_sqft_avg_5_prior,
        NTILE(10) OVER w_hist AS running_count_sqft_bucket_bucket
    FROM deduped
    WHERE running_count_sqft_bucket BETWEEN 6 AND 8
    WINDOW w_hist AS (PARTITION BY region ORDER BY running_count_sqft_bucket ASC, id ASC)
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, region
    FROM scored_stores
    EXCEPT DISTINCT
    SELECT id, region
    FROM stores
    WHERE sqft IS NOT NULL
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored_stores s
    JOIN remaining_keys k ON s.id = k.id AND s.region = k.region
)
SELECT
    id,
    region,
    store_name_city_key,
    running_sum_sqft,
    running_min_sqft,
    running_count_sqft_bucket,
    running_min_sqft_share,
    running_min_sqft_avg_5_prior,
    running_count_sqft_bucket_bucket
FROM verified;
