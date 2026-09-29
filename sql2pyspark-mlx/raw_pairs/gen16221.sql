WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        DATE_TRUNC('year', last_restock) AS last_restock_period
    FROM inventory
    WHERE last_restock >= TO_DATE('2023-02-28')
      AND quantity >= 81
),
joined AS (
    -- Attach stores attributes
    SELECT
        src.id,
        src.store_id,
        src.product_id,
        src.last_restock_period,
        b.store_name,
        b.sqft,
        b.region
    FROM cleaned src
    LEFT JOIN stores b
        ON src.store_id = b.id
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(last_restock_period) OVER (PARTITION BY store_id ORDER BY last_restock_period ASC, id ASC) AS prev_last_restock_period
    FROM joined
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_last_restock_period IS NULL OR last_restock_period > prev_last_restock_period + INTERVAL '30' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM with_prev
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        store_id,
        last_restock_period,
        store_name,
        region,
        SUM(session_start_flag) OVER (PARTITION BY store_id ORDER BY last_restock_period ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        last_restock_period,
        region,
        NTILE(4) OVER (PARTITION BY DATE_TRUNC('month', last_restock_period), region ORDER BY burst_id ASC, id ASC) AS burst_id_bucket,
        SUM(burst_id) OVER (PARTITION BY YEAR(last_restock_period), region ORDER BY last_restock_period ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_burst_id,
        ROW_NUMBER() OVER (ORDER BY burst_id ASC, id ASC) AS row_seq
    FROM islands
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY region ORDER BY burst_id_bucket DESC NULLS FIRST, id ASC) = 1
)
-- Aggregate with conditional metrics
SELECT
    cumulative_burst_id,
    DATE_TRUNC('QUARTER', last_restock_period) AS quarter_start,
    AVG(row_seq) FILTER (WHERE region = 'North') AS north_avg_row_seq,
    SORT_ARRAY(COLLECT_SET(region)) AS region_list,
    SORT_ARRAY(COLLECT_SET(region)) AS region_list_2
FROM ranked
GROUP BY GROUPING SETS ((cumulative_burst_id, DATE_TRUNC('QUARTER', last_restock_period)), (cumulative_burst_id))
HAVING AVG(row_seq) FILTER (WHERE region = 'North') > 0
ORDER BY north_avg_row_seq ASC NULLS LAST
