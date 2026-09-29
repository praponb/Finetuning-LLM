WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(sqft) OVER w_grp AS sqft_avg_3,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY DATE_TRUNC('WEEK', opened_date), region), 4) AS sqft_share
    FROM stores
    WINDOW w_grp AS (PARTITION BY MONTH(opened_date), region ORDER BY sqft, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM metrics
    EXCEPT
    SELECT id
    FROM stores
    WHERE sqft BETWEEN 7623 AND 7975
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    JOIN remaining_keys k ON s.id = k.id
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM verified s
    WHERE EXISTS (
            SELECT 1
            FROM orders x
            WHERE x.store_id = s.id
              AND x.order_date <= s.opened_date
        )
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_name,
        opened_date,
        sqft,
        sqft_share,
        sqft_avg_3 + sqft AS sqft_avg_3_plus_sqft,
        COALESCE(region, 'Unknown') AS region_filled
    FROM eligible
    WHERE opened_date >= DATE '2023-04-28'
      AND region IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY sqft_avg_3_plus_sqft DESC NULLS LAST) AS sqft_avg_3_plus_sqft_dense_rank,
        CUME_DIST() OVER (PARTITION BY DATE_TRUNC('month', opened_date) ORDER BY sqft_share DESC NULLS FIRST) AS sqft_share_cume_dist,
        DENSE_RANK() OVER (ORDER BY sqft_avg_3_plus_sqft DESC NULLS FIRST) AS sqft_avg_3_plus_sqft_dense_rank_2,
        DENSE_RANK() OVER (ORDER BY sqft_avg_3_plus_sqft NULLS FIRST) AS sqft_avg_3_plus_sqft_dense_rank_3,
        CUME_DIST() OVER (ORDER BY sqft_avg_3_plus_sqft DESC) AS sqft_avg_3_plus_sqft_cume_dist
    FROM cleaned
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_name,
        sqft,
        sqft_share,
        sqft_avg_3_plus_sqft,
        region_filled,
        sqft_avg_3_plus_sqft_dense_rank,
        sqft_avg_3_plus_sqft_dense_rank_3,
        sqft_avg_3_plus_sqft_cume_dist,
        ADD_MONTHS(opened_date, 3) AS opened_date_plus_months,
        sqft_share / NULLIF(sqft_avg_3_plus_sqft_cume_dist, 0) AS sqft_share_per_sqft_avg_3_plus_sqft_cume_dist
    FROM enriched
    WHERE sqft_share_cume_dist >= 0.69
)
-- Multi-level aggregation
SELECT
    CASE WHEN sqft_share >= 0.61 THEN 'LARGE' ELSE 'SMALL' END AS sqft_share_tier,
    DATE_TRUNC('YEAR', opened_date_plus_months) AS year_start,
    AVG(sqft_avg_3_plus_sqft_dense_rank_3) AS avg_sqft_avg_3_plus_sqft_dense_rank_3,
    PERCENTILE_APPROX(sqft_share, ARRAY(0.25, 0.5, 0.75)) AS sqft_share_quartiles,
    AVG(sqft_avg_3_plus_sqft_dense_rank_3) AS avg_sqft_avg_3_plus_sqft_dense_rank_3_2,
    GROUPING(CASE WHEN sqft_share >= 0.61 THEN 'LARGE' ELSE 'SMALL' END) AS gid
FROM base
GROUP BY GROUPING SETS ((CASE WHEN sqft_share >= 0.61 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('YEAR', opened_date_plus_months)), (CASE WHEN sqft_share >= 0.61 THEN 'LARGE' ELSE 'SMALL' END))
