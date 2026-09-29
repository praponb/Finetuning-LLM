WITH with_windows AS (
    SELECT
        id,
        store_name,
        city,
        sqft,
        LAG(sqft) OVER (PARTITION BY city ORDER BY opened_date, id) AS prev_sqft,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date, id) AS row_seq,
        MIN(sqft) OVER (PARTITION BY city, region) AS city_min_sqft
    FROM stores
),
scored AS (
    SELECT
        *,
        AVG(sqft) OVER w_part AS sqft_avg_5_prior,
        SUM(prev_sqft) OVER w_ordered AS cumulative_prev_sqft,
        ROUND(city_min_sqft / SUM(city_min_sqft) OVER (PARTITION BY city), 4) AS city_min_sqft_share,
        ROUND(prev_sqft / SUM(prev_sqft) OVER (PARTITION BY city), 4) AS prev_sqft_share
    FROM with_windows
    WHERE city IN ('Chiang Mai', 'Berlin', 'Singapore')
    WINDOW w_part AS (PARTITION BY city ORDER BY city_min_sqft, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING), w_ordered AS (PARTITION BY city ORDER BY prev_sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
kept AS (
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.quantity > 172
        )
),
metrics AS (
    SELECT
        *,
        LAG(city_min_sqft_share, 1) OVER (PARTITION BY city ORDER BY city_min_sqft_share, id) AS prev_city_min_sqft_share,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY cumulative_prev_sqft DESC, id) AS seq_no,
        AVG(sqft) OVER (PARTITION BY city ORDER BY row_seq, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS sqft_avg_6_prior,
        NTILE(4) OVER (PARTITION BY city ORDER BY prev_sqft_share DESC, id) AS prev_sqft_share_bucket
    FROM kept
),
daily AS (
    SELECT
        city,
        CASE WHEN sqft >= 16135 THEN 'PREMIUM' ELSE 'STANDARD' END AS sqft_tier,
        seq_no,
        BIT_AND(sqft) AS sqft_bit_and,
        COUNT(DISTINCT id) AS distinct_ids
    FROM metrics
    GROUP BY GROUPING SETS ((city, CASE WHEN sqft >= 16135 THEN 'PREMIUM' ELSE 'STANDARD' END, seq_no), (city, CASE WHEN sqft >= 16135 THEN 'PREMIUM' ELSE 'STANDARD' END), (city), ())
    HAVING BIT_AND(sqft) > 1
)
SELECT
    city,
    sqft_tier,
    sqft_bit_and,
    distinct_ids,
    CASE WHEN city RLIKE '^(London|Chiang Mai)$' THEN 'PRIORITY' WHEN city RLIKE '^(Bangkok|New York)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS city_zone
FROM daily
ORDER BY sqft_bit_and DESC;
