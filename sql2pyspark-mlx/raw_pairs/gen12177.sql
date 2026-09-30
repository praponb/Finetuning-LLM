WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CEIL(units) AS units_whole,
        DATEDIFF(CURRENT_DATE(), sale_date) / 365.25 AS years_since_sale_date
    FROM store_sales
    WHERE sale_date >= DATE '2023-05-23'
      AND store_id IN (1, 2, 4, 6)
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        sale_date,
        units,
        units_whole
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY units NULLS FIRST, id) = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(units_whole, 2) OVER (ORDER BY sale_date, id) AS next_units_whole,
        DENSE_RANK() OVER (ORDER BY units_whole DESC NULLS LAST) AS units_whole_dense_rank
    FROM latest
    WHERE units = 38
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY units_whole DESC NULLS LAST) AS units_whole_percent_rank,
        NTILE(10) OVER w_recent AS units_whole_bucket,
        AVG(units_whole_dense_rank) OVER (ORDER BY units_whole, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS units_whole_dense_rank_avg_2,
        SUM(next_units_whole) OVER (ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_next_units_whole,
        LAG(next_units_whole, 2) OVER (PARTITION BY DATE_TRUNC('YEAR', sale_date) ORDER BY next_units_whole, id) AS prev_next_units_whole
    FROM metrics
    WINDOW w_recent AS (ORDER BY units_whole DESC, id)
),
enriched_store_sales AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY next_units_whole, id) AS event_seq,
        CUME_DIST() OVER (ORDER BY units_whole_dense_rank_avg_2 NULLS LAST) AS units_whole_dense_rank_avg_2_cume_dist,
        LAST_VALUE(prev_next_units_whole, TRUE) OVER (ORDER BY prev_next_units_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_prev_next_units_whole
    FROM enriched
),
enriched_store_sales_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        units,
        next_units_whole,
        units_whole_dense_rank,
        units_whole_dense_rank_avg_2,
        cumulative_next_units_whole,
        prev_next_units_whole,
        event_seq,
        units_whole_dense_rank_avg_2_cume_dist,
        last_known_prev_next_units_whole,
        MAX(last_known_prev_next_units_whole) OVER w_running AS running_max_last_known_prev_next_units_whole,
        COUNT(last_known_prev_next_units_whole) OVER w_running AS running_count_last_known_prev_next_units_whole,
        SUM(last_known_prev_next_units_whole) OVER w_running AS running_sum_last_known_prev_next_units_whole
    FROM enriched_store_sales
    WINDOW w_running AS (ORDER BY units_whole_percent_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    next_units_whole,
    units_whole_dense_rank,
    units_whole_dense_rank_avg_2,
    prev_next_units_whole,
    event_seq,
    running_max_last_known_prev_next_units_whole,
    running_count_last_known_prev_next_units_whole,
    running_sum_last_known_prev_next_units_whole
FROM enriched_store_sales_2;
