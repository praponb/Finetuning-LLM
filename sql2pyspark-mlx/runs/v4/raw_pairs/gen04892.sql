WITH matched_keys AS (
    -- Set intersection on keys
    SELECT city, id
    FROM stores
    INTERSECT ALL
    SELECT city, id
    FROM stores
    WHERE region NOT IN ('East', 'West')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM stores s
    INNER JOIN matched_keys k ON s.city = k.city AND s.id = k.id
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM reconciled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.store_id = s.id
              AND x.sale_date < s.opened_date
        )
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY sqft DESC NULLS FIRST) AS sqft_dense_rank,
        AVG(sqft) OVER w_grp AS region_avg_sqft,
        PERCENT_RANK() OVER (ORDER BY sqft DESC) AS sqft_percent_rank,
        RANK() OVER (PARTITION BY city, region ORDER BY sqft ASC) AS sqft_rank,
        RANK() OVER (PARTITION BY region ORDER BY sqft DESC NULLS LAST) AS sqft_rank_2
    FROM kept
    WINDOW w_grp AS (PARTITION BY region, city)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY region, city ORDER BY sqft ASC) AS sqft_rank_3,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY sqft_percent_rank ASC) AS sqft_percent_rank_dense_rank,
        DENSE_RANK() OVER (ORDER BY sqft_rank_2 DESC NULLS FIRST) AS sqft_rank_2_dense_rank,
        RANK() OVER (PARTITION BY region ORDER BY sqft_rank DESC NULLS FIRST) AS sqft_rank_rank
    FROM with_windows
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        sqft_dense_rank,
        region_avg_sqft,
        sqft_rank,
        sqft_rank_2,
        sqft_rank_3,
        sqft_percent_rank_dense_rank,
        sqft_rank_2_dense_rank,
        sqft_rank_rank,
        ROUND(sqft_rank_3, 0) AS sqft_rank_3_rounded
    FROM enriched
    WHERE opened_date > DATE '2023-07-22'
      AND region IS NOT NULL
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('MONTH', opened_date) AS activity_month,
    MIN(sqft_rank) AS min_sqft_rank,
    SUM(sqft_rank_3) AS total_sqft_rank_3,
    COUNT(DISTINCT city) AS distinct_cities,
    BIT_AND(sqft) AS sqft_bit_and,
    COUNT(CASE WHEN region = 'Central' THEN 1 END) AS central_count
FROM cleaned
GROUP BY DATE_TRUNC('MONTH', opened_date)
