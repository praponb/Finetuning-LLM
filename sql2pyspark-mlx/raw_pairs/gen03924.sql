WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        units,
        revenue,
        CEIL(revenue) AS revenue_whole
    FROM store_sales
    WHERE product_id IN (2, 9)
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM filtered
    QUALIFY RANK() OVER (PARTITION BY product_id ORDER BY revenue_whole) = 1
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        revenue,
        revenue_whole,
        CAST(revenue_whole AS STRING) AS revenue_whole_num
    FROM deduped
    WHERE product_id = 4
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        revenue,
        ROW_NUMBER() OVER (ORDER BY revenue_whole, id) AS event_seq,
        LEAD(revenue, 1, 0) OVER w_hist AS next_revenue,
        NTILE(10) OVER w_hist_2 AS revenue_bucket,
        LAG(revenue_whole, 1) OVER w_grp AS prev_revenue_whole,
        LAG(revenue_whole, 2) OVER w_grp AS prev_revenue_whole_2
    FROM base
    WHERE revenue < 3400
    WINDOW w_hist AS (PARTITION BY store_id ORDER BY revenue, id), w_hist_2 AS (ORDER BY revenue, id), w_grp AS (PARTITION BY store_id ORDER BY revenue_whole, id)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        event_seq,
        next_revenue,
        prev_revenue_whole,
        prev_revenue_whole_2,
        LEAD(event_seq) OVER (ORDER BY prev_revenue_whole_2, id) AS next_event_seq,
        LAG(event_seq) OVER (ORDER BY prev_revenue_whole_2, id) AS prev_event_seq,
        LEAD(revenue) OVER (ORDER BY prev_revenue_whole_2, id) AS next_revenue_2,
        ROW_NUMBER() OVER (ORDER BY prev_revenue_whole_2, id) AS event_seq_2,
        LAST_VALUE(prev_revenue_whole) IGNORE NULLS OVER (ORDER BY revenue_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_prev_revenue_whole
    FROM enriched
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(event_seq_2, 1, 0) OVER (ORDER BY event_seq, id) AS next_event_seq_2,
        ROW_NUMBER() OVER (ORDER BY next_event_seq, id) AS seq_no,
        NTILE(5) OVER (ORDER BY next_revenue_2, id) AS next_revenue_2_bucket
    FROM metrics
),
base_store_sales AS (
    -- Keep recent, valid rows
    SELECT *, next_revenue_2 - seq_no AS next_revenue_2_minus_seq_no
    FROM scored
    WHERE prev_revenue_whole < 58
),
summary AS (
    -- Grouped metrics
    SELECT
        CASE WHEN prev_revenue_whole_2 >= 90 THEN 'HIGH' ELSE 'LOW' END AS prev_revenue_whole_2_tier,
        seq_no,
        PERCENTILE_APPROX(last_known_prev_revenue_whole, ARRAY(0.25, 0.5, 0.75)) AS last_known_prev_revenue_whole_quartiles,
        COUNT(DISTINCT id) AS distinct_ids,
        APPROX_PERCENTILE(last_known_prev_revenue_whole, 0.5) AS last_known_prev_revenue_whole_p50,
        MIN(next_revenue_2) AS min_next_revenue_2,
        APPROX_PERCENTILE(prev_revenue_whole, 0.5) AS prev_revenue_whole_p50,
        APPROX_PERCENTILE(prev_revenue_whole, 0.95) AS prev_revenue_whole_p95
    FROM base_store_sales
    GROUP BY CASE WHEN prev_revenue_whole_2 >= 90 THEN 'HIGH' ELSE 'LOW' END, seq_no
)
SELECT
    prev_revenue_whole_2_tier,
    seq_no,
    last_known_prev_revenue_whole_quartiles,
    distinct_ids,
    last_known_prev_revenue_whole_p50,
    prev_revenue_whole_p50,
    prev_revenue_whole_p95
FROM summary
ORDER BY seq_no DESC
