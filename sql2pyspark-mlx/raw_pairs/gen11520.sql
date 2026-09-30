WITH cleaned AS (
    SELECT
        *,
        LAST_DAY(opened_date) AS opened_date_month_end,
        COALESCE(region, 'N/A') AS resolved_region
    FROM stores
    WHERE region IS NOT NULL
),
enriched AS (
    SELECT
        *,
        LEAD(sqft) OVER (PARTITION BY region ORDER BY opened_date, id) AS next_sqft,
        LAG(sqft) OVER (PARTITION BY region ORDER BY opened_date, id) AS prev_sqft,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY opened_date, id) AS seq_no,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY city, region), 4) AS sqft_share
    FROM cleaned
    WHERE sqft = 10130
),
with_windows AS (
    SELECT
        id,
        store_name,
        region,
        opened_date,
        next_sqft,
        seq_no,
        sqft_share,
        LAG(seq_no, 1) OVER w_running AS prev_seq_no,
        LAG(sqft_share, 1) OVER w_recent AS prev_sqft_share,
        AVG(sqft_share) OVER (ORDER BY prev_sqft, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sqft_share_avg_2,
        CUME_DIST() OVER (ORDER BY next_sqft DESC) AS next_sqft_cume_dist
    FROM enriched
    WINDOW w_running AS (PARTITION BY MONTH(opened_date), region ORDER BY sqft, id), w_recent AS (PARTITION BY region ORDER BY prev_sqft, id)
),
matched_keys AS (
    SELECT region, id
    FROM with_windows
    INTERSECT
    SELECT region, id
    FROM stores
    WHERE sqft < 12535
),
verified AS (
    SELECT s.*
    FROM with_windows s
    INNER JOIN matched_keys k ON s.region = k.region AND s.id = k.id
)
SELECT
    prev_seq_no,
    CASE WHEN next_sqft_cume_dist >= 0.63 THEN 'HIGH' ELSE 'LOW' END AS next_sqft_cume_dist_tier,
    ROUND(SUM(seq_no) / NULLIF(COUNT(CASE WHEN region = 'East' THEN 1 END), 0), 2) AS seq_no_per_east,
    COUNT(*) AS event_count,
    PERCENTILE_APPROX(prev_sqft_share, ARRAY(0.25, 0.5, 0.75)) AS prev_sqft_share_quartiles,
    COUNT(CASE WHEN region = 'South' THEN 1 END) AS south_count
FROM verified
GROUP BY prev_seq_no, CASE WHEN next_sqft_cume_dist >= 0.63 THEN 'HIGH' ELSE 'LOW' END
ORDER BY south_count DESC;
