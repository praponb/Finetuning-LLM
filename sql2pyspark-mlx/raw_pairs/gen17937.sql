WITH valid AS (
    SELECT
        id,
        product_id,
        units,
        revenue,
        CAST(revenue AS INT) AS revenue_int
    FROM store_sales
    WHERE revenue <= 1500
),
with_windows AS (
    SELECT
        id,
        product_id,
        units,
        revenue_int,
        LAST_VALUE(revenue_int, TRUE) OVER w_grp AS last_known_revenue_int,
        ROW_NUMBER() OVER w_recent AS seq_no,
        PERCENT_RANK() OVER w_running AS revenue_int_percent_rank
    FROM valid
    WINDOW w_grp AS (PARTITION BY product_id ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent AS (ORDER BY revenue, id), w_running AS (PARTITION BY product_id ORDER BY revenue_int DESC NULLS LAST)
),
scored AS (
    SELECT
        *,
        AVG(units) OVER (PARTITION BY product_id ORDER BY revenue_int_percent_rank, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS units_avg_3,
        SUM(units) OVER (PARTITION BY product_id ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_units
    FROM with_windows
    WHERE seq_no < 3
),
with_windows_store_sales AS (
    SELECT
        *,
        NTILE(4) OVER (ORDER BY seq_no, id) AS seq_no_bucket,
        PERCENT_RANK() OVER w_part AS revenue_int_percent_rank_2,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_known_revenue_int, id) AS seq_no_2
    FROM scored
    WINDOW w_part AS (PARTITION BY product_id ORDER BY revenue_int DESC NULLS LAST)
),
metrics AS (
    SELECT
        id,
        units,
        revenue_int,
        last_known_revenue_int,
        seq_no,
        running_units,
        MAX(running_units) OVER w_running AS product_id_max_running_units,
        AVG(units_avg_3) OVER (ORDER BY units_avg_3, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS units_avg_3_avg_3_prior,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY seq_no_bucket DESC, id) AS row_seq
    FROM with_windows_store_sales
    WINDOW w_running AS (PARTITION BY product_id)
)
SELECT
    CASE WHEN revenue_int >= 70 THEN 'HIGH' ELSE 'LOW' END AS revenue_int_tier,
    CASE WHEN units_avg_3_avg_3_prior >= 33 THEN 'PREMIUM' ELSE 'STANDARD' END AS units_avg_3_avg_3_prior_tier,
    BIT_OR(units) AS units_bit_or,
    ROUND(AVG(row_seq), 2) AS avg_row_seq
FROM metrics
GROUP BY CASE WHEN revenue_int >= 70 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN units_avg_3_avg_3_prior >= 33 THEN 'PREMIUM' ELSE 'STANDARD' END;
