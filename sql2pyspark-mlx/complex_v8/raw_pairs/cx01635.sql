WITH cleaned_warehouses AS (
    -- Filter warehouses
    SELECT
        id,
        wh_code,
        city,
        capacity,
        ABS(capacity - 11506) AS capacity_distance,
        SUBSTRING(TRIM(city), 1, 2) AS city_prefix
    FROM warehouses
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER w_city AS rows_in_city,
        ROW_NUMBER() OVER w_all_by_capacity_distance AS capacity_distance_seq,
        ROUND(capacity - AVG(capacity) OVER (PARTITION BY city), 2) AS capacity_vs_city_avg,
        FIRST_VALUE(capacity_distance) IGNORE NULLS OVER (PARTITION BY city ORDER BY capacity ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_capacity_distance,
        ROUND(CUME_DIST() OVER w_city_by_capacity, 3) AS capacity_cume_dist,
        NTILE(5) OVER w_city_by_capacity AS capacity_quintile,
        LEAD(capacity) OVER (PARTITION BY city ORDER BY capacity ASC, id) AS next_capacity,
        ROUND(capacity / NULLIF(SUM(capacity) OVER (PARTITION BY city), 0), 4) AS capacity_share_of_city,
        capacity_distance - LAG(capacity_distance) OVER (ORDER BY capacity_distance NULLS LAST, id) AS capacity_distance_change,
        ROUND(capacity_distance - AVG(capacity_distance) OVER (PARTITION BY city), 2) AS capacity_distance_vs_city_avg,
        NTH_VALUE(capacity, 2) OVER (PARTITION BY city ORDER BY capacity ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_capacity,
        ROW_NUMBER() OVER w_city_by_capacity_distance AS seq_no,
        MAX(capacity_distance) OVER (PARTITION BY city) AS city_max_capacity_distance
    FROM cleaned_warehouses
    WINDOW w_city AS (PARTITION BY city), w_all_by_capacity_distance AS (ORDER BY capacity_distance NULLS LAST, id), w_city_by_capacity AS (PARTITION BY city ORDER BY capacity ASC, id), w_city_by_capacity_distance AS (PARTITION BY city ORDER BY capacity_distance DESC, id)
),
sequenced_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        capacity,
        city_prefix,
        rows_in_city,
        capacity_cume_dist,
        capacity_quintile,
        next_capacity,
        capacity_share_of_city,
        capacity_distance_change,
        capacity_distance_vs_city_avg,
        second_capacity,
        seq_no,
        city_max_capacity_distance,
        LEAD(capacity_share_of_city, 1, 0) OVER w_all_by_capacity_distance_2 AS next_capacity_share_of_city,
        ROUND(CUME_DIST() OVER w_all_by_seq_no_ties, 3) AS seq_no_cume_dist,
        NTH_VALUE(capacity, 2) OVER (PARTITION BY seq_no, city ORDER BY next_capacity DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_capacity_2,
        COUNT(*) OVER w_seq_no_capacity_quintile AS rows_in_seq_no_capacity_quintile,
        COUNT(*) OVER (PARTITION BY seq_no, city ORDER BY next_capacity DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        DENSE_RANK() OVER w_city_by_next_capacity_ties AS next_capacity_dense_rank,
        ROUND(AVG(capacity_distance_change) OVER (ORDER BY seq_no DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS capacity_distance_change_avg_4
    FROM sequenced
    WINDOW w_all_by_capacity_distance_2 AS (ORDER BY capacity_distance, id), w_all_by_seq_no_ties AS (ORDER BY seq_no DESC), w_seq_no_capacity_quintile AS (PARTITION BY seq_no, capacity_quintile), w_city_by_next_capacity_ties AS (PARTITION BY city ORDER BY next_capacity NULLS LAST)
),
grouped AS (
    -- Group-level totals
    SELECT
        CASE WHEN seq_no_cume_dist >= 0.7 THEN 'A' WHEN seq_no_cume_dist >= 0.3 THEN 'B' ELSE 'C' END AS seq_no_cume_dist_tier,
        ROUND(STDDEV(capacity_share_of_city), 2) AS stddev_capacity_share_of_city,
        SUM(capacity_distance_change) AS total_capacity_distance_change,
        MIN(city_max_capacity_distance) AS min_city_max_capacity_distance,
        ROUND(SUM(capacity_cume_dist), 2) AS total_capacity_cume_dist,
        COUNT(*) AS cnt
    FROM sequenced_2
    GROUP BY CASE WHEN seq_no_cume_dist >= 0.7 THEN 'A' WHEN seq_no_cume_dist >= 0.3 THEN 'B' ELSE 'C' END
    HAVING MAX(capacity_quintile) > 4
)
SELECT
    seq_no_cume_dist_tier,
    stddev_capacity_share_of_city,
    total_capacity_cume_dist,
    ABS(cnt - 9) AS cnt_distance
FROM grouped
ORDER BY seq_no_cume_dist_tier
