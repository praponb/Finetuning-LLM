WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY product_id ORDER BY sale_date NULLS FIRST) AS latest_rank
    FROM store_sales
    QUALIFY latest_rank = 1
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        sale_date,
        units,
        revenue,
        ROUND(units * 1.15, 2) AS adjusted_units
    FROM ranked
    WHERE sale_date > TO_DATE('2023-08-01')
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(units, 2, 0) OVER (ORDER BY revenue, id) AS next_units,
        SUM(revenue) OVER (PARTITION BY DATE_TRUNC('WEEK', sale_date) ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_revenue,
        DENSE_RANK() OVER w_part AS adjusted_units_dense_rank
    FROM base
    WINDOW w_part AS (PARTITION BY YEAR(sale_date) ORDER BY adjusted_units NULLS FIRST)
),
metrics_store_sales AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY revenue DESC, id) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY MONTH(sale_date) ORDER BY next_units DESC, id) AS event_seq_2,
        LEAD(adjusted_units_dense_rank, 2, 0) OVER (ORDER BY adjusted_units, id) AS next_adjusted_units_dense_rank,
        SUM(next_units) OVER (ORDER BY adjusted_units_dense_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_next_units
    FROM metrics
    WHERE units < 32
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM metrics_store_sales
    EXCEPT DISTINCT
    SELECT id
    FROM store_sales
    WHERE YEAR(sale_date) >= 2025
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics_store_sales s
    JOIN matched_keys k ON s.id = k.id
),
metrics_store_sales_2 AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY running_revenue NULLS LAST) AS running_revenue_cume_dist,
        PERCENT_RANK() OVER (ORDER BY event_seq DESC) AS event_seq_percent_rank
    FROM verified
),
daily AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('YEAR', sale_date) AS year_start,
        CASE WHEN adjusted_units_dense_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS adjusted_units_dense_rank_tier,
        COUNT(*) AS num_rows,
        COUNT(*) AS event_count,
        COUNT(*) AS cnt,
        ROUND(AVG(cumulative_next_units), 2) AS avg_cumulative_next_units,
        COUNT(*) AS event_count_2,
        GROUPING_ID() AS gid
    FROM metrics_store_sales_2
    GROUP BY GROUPING SETS ((DATE_TRUNC('YEAR', sale_date), CASE WHEN adjusted_units_dense_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END), (DATE_TRUNC('YEAR', sale_date)), ())
)
SELECT
    year_start,
    event_count,
    cnt,
    avg_cumulative_next_units,
    gid
FROM daily
ORDER BY cnt DESC, event_count DESC
