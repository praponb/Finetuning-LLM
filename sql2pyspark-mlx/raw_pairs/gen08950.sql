WITH base AS (
    SELECT
        id,
        store_name,
        region,
        opened_date,
        FLOOR(sqft) AS sqft_whole,
        CONCAT_WS('-', store_name, region) AS store_name_region_key,
        COALESCE(region, 'UNKNOWN') AS region_clean
    FROM stores
    WHERE region IS NOT NULL
),
filtered AS (
    SELECT
        *,
        CASE WHEN sqft_whole >= 89 THEN 'High' WHEN sqft_whole >= 13 THEN 'Medium' ELSE 'Low' END AS sqft_whole_tier,
        CONCAT(store_name, ' ', region) AS store_name_region,
        COALESCE(region, 'OTHER') AS region_filled
    FROM base
    WHERE opened_date > DATE '2023-09-14'
      AND region IS NOT NULL
),
matched_keys AS (
    SELECT region, id
    FROM filtered
    INTERSECT ALL
    SELECT region, id
    FROM stores
    WHERE LENGTH(store_name) >= 5
),
verified AS (
    SELECT s.*
    FROM filtered s
    JOIN matched_keys k ON s.region = k.region AND s.id = k.id
),
cleaned AS (
    SELECT
        *,
        LENGTH(store_name) AS store_name_length,
        COALESCE(region, 'DIRECT') AS region_filled_2
    FROM verified
    WHERE region IS NOT NULL
),
enriched AS (
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY region ORDER BY store_name_length DESC NULLS FIRST) AS store_name_length_cume_dist,
        RANK() OVER w_hist AS store_name_length_rank,
        DENSE_RANK() OVER (PARTITION BY MONTH(opened_date), region ORDER BY store_name_length ASC NULLS FIRST) AS store_name_length_dense_rank,
        COUNT(sqft_whole) OVER w_running AS region_count_sqft_whole,
        MAX(sqft_whole) OVER w_running AS region_max_sqft_whole
    FROM cleaned
    WINDOW w_hist AS (ORDER BY store_name_length ASC), w_running AS (PARTITION BY region)
),
cleaned_stores AS (
    SELECT
        id,
        region,
        opened_date,
        sqft_whole,
        store_name_region_key,
        region_clean,
        sqft_whole_tier,
        region_filled,
        region_filled_2,
        store_name_length_cume_dist,
        store_name_length_rank,
        store_name_length_dense_rank,
        region_max_sqft_whole,
        LEAST(store_name_length_dense_rank, region_max_sqft_whole) AS least_store_name_length_dense_rank_region_max_sqft_whole
    FROM enriched
    WHERE opened_date >= DATE '2023-08-09'
      AND store_name_length <= 38
)
SELECT
    DATE_TRUNC('WEEK', opened_date) AS week_start,
    APPROX_PERCENTILE(least_store_name_length_dense_rank_region_max_sqft_whole, 0.9) AS least_store_name_length_dense_rank_region_max_sqft_whole_p90,
    APPROX_PERCENTILE(sqft_whole, ARRAY(0.25, 0.5, 0.75)) AS sqft_whole_quartiles
FROM cleaned_stores
GROUP BY DATE_TRUNC('WEEK', opened_date)
ORDER BY least_store_name_length_dense_rank_region_max_sqft_whole_p90 DESC NULLS LAST;
