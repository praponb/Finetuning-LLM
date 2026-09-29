WITH valid AS (
    SELECT
        id,
        store_name,
        region,
        opened_date,
        sqft,
        CASE WHEN region = 'East' THEN 1 ELSE 0 END AS is_east
    FROM stores
    WHERE region IS NOT NULL
),
kept AS (
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.store_id = s.id
              AND x.sale_date <= s.opened_date
        )
),
with_windows AS (
    SELECT
        id,
        store_name,
        region,
        opened_date,
        sqft,
        is_east,
        RANK() OVER (ORDER BY sqft DESC) AS sqft_rank,
        ROW_NUMBER() OVER w_ordered AS event_seq,
        SUM(sqft) OVER w_grp AS cumulative_sqft
    FROM kept
    WINDOW w_ordered AS (ORDER BY sqft DESC, id), w_grp AS (PARTITION BY region ORDER BY is_east, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows_stores AS (
    SELECT
        id,
        store_name,
        region,
        opened_date,
        sqft,
        is_east,
        sqft_rank,
        event_seq,
        cumulative_sqft,
        AVG(event_seq) OVER w_running AS event_seq_avg_5_prior,
        SUM(event_seq) OVER w_ordered AS region_sum_event_seq,
        LAG(is_east, 1) OVER (PARTITION BY region ORDER BY sqft, id) AS prev_is_east
    FROM with_windows
    WINDOW w_running AS (PARTITION BY region ORDER BY opened_date, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING), w_ordered AS (PARTITION BY region)
)
SELECT
    region,
    MAX(sqft) FILTER (WHERE region = 'North') AS north_max_sqft,
    COUNT(*) FILTER (WHERE region = 'North') AS north_rows
FROM with_windows_stores
GROUP BY 1
ORDER BY north_max_sqft;
