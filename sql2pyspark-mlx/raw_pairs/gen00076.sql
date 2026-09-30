WITH cleaned AS (
    SELECT
        id,
        name,
        email,
        city,
        segment,
        is_active,
        LENGTH(segment) AS segment_length,
        COALESCE(segment, 'OTHER') AS segment_clean
    FROM customers
    WHERE country IN ('TH', 'US', 'SG')
),
kept_keys AS (
    SELECT city
    FROM cleaned
    EXCEPT DISTINCT
    SELECT city
    FROM customers
    WHERE name LIKE 'Gra%'
),
verified AS (
    SELECT s.*
    FROM cleaned s
    JOIN kept_keys k ON s.city = k.city
),
scored AS (
    SELECT *, RANK() OVER (PARTITION BY city ORDER BY segment_length NULLS LAST) AS segment_length_rank
    FROM verified
),
scored_customers AS (
    SELECT
        email,
        segment,
        is_active,
        segment_length,
        segment_length_rank,
        SUM(segment_length) OVER (PARTITION BY city) AS city_sum_segment_length,
        MIN(segment_length) OVER w_part AS city_min_segment_length,
        DENSE_RANK() OVER (PARTITION BY city, segment ORDER BY segment_length NULLS LAST) AS segment_length_dense_rank,
        SUM(segment_length) OVER (PARTITION BY segment, city) AS segment_sum_segment_length,
        RANK() OVER w_ordered AS segment_length_rank_2
    FROM scored
    WHERE segment IN ('SMB', 'Consumer')
    WINDOW w_part AS (PARTITION BY city, segment), w_ordered AS (ORDER BY segment_length DESC NULLS LAST)
)
SELECT
    segment,
    COUNT(DISTINCT segment) AS distinct_segments,
    SIZE(COLLECT_SET(segment)) AS num_segments,
    COUNT_IF(segment = 'SMB') AS smb_events,
    PERCENTILE_APPROX(segment_sum_segment_length, ARRAY(0.25, 0.5, 0.75)) AS segment_sum_segment_length_quartiles
FROM scored_customers
GROUP BY segment
ORDER BY smb_events DESC NULLS LAST;
