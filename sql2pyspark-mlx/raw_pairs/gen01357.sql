WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(sqft, 0) AS sqft_filled,
        COALESCE(region, 'OTHER') AS resolved_region
    FROM stores
    WHERE opened_date > TO_DATE('2023-03-09')
      AND region IS NOT NULL
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, city
    FROM filtered
    EXCEPT
    SELECT id, city
    FROM stores
    WHERE region <> 'Central'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    JOIN kept_keys k ON s.id = k.id AND s.city = k.city
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        store_name,
        city,
        region,
        sqft,
        CUME_DIST() OVER (PARTITION BY city ORDER BY sqft NULLS LAST) AS sqft_cume_dist,
        RANK() OVER (PARTITION BY city ORDER BY sqft_filled NULLS FIRST) AS sqft_filled_rank,
        DENSE_RANK() OVER (ORDER BY sqft DESC) AS sqft_dense_rank
    FROM reconciled
),
metrics_stores AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        city,
        region,
        sqft_filled_rank,
        COUNT(sqft_dense_rank) OVER (PARTITION BY region, city) AS region_count_sqft_dense_rank
    FROM metrics
    WHERE city IN ('Bangkok', 'Berlin')
),
stats AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN sqft_filled_rank >= 8 THEN 'HIGH' ELSE 'LOW' END AS sqft_filled_rank_tier,
        SORT_ARRAY(COLLECT_SET(region)) AS region_list,
        SORT_ARRAY(COLLECT_SET(region)) AS region_list_2,
        APPROX_PERCENTILE(sqft_filled_rank, ARRAY(0.25, 0.5, 0.75)) AS sqft_filled_rank_quartiles
    FROM metrics_stores
    GROUP BY CASE WHEN sqft_filled_rank >= 8 THEN 'HIGH' ELSE 'LOW' END
)
SELECT
    sqft_filled_rank_tier,
    region_list,
    region_list_2
FROM stats;
