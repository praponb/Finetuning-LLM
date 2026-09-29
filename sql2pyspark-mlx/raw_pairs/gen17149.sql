WITH with_dims AS (
    SELECT
        e.*,
        p.order_date,
        p.amount
    FROM shipments e
    LEFT JOIN orders p
        ON e.order_id <=> p.id
),
cleaned AS (
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        amount,
        UPPER(status) AS status_upper
    FROM with_dims
    WHERE order_id IS NOT NULL
      AND shipped_date >= TO_DATE('2023-06-28')
),
with_windows AS (
    SELECT
        id,
        order_id,
        shipped_date,
        cost,
        MIN(amount) OVER (PARTITION BY carrier ORDER BY cost ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_amount,
        COUNT(amount) OVER (PARTITION BY carrier ORDER BY cost ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_amount,
        AVG(amount) OVER (PARTITION BY carrier ORDER BY cost ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_amount,
        SUM(amount) OVER (PARTITION BY carrier ORDER BY cost ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_amount
    FROM cleaned
    WHERE carrier = 'Kerry'
),
enriched AS (
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY order_id ORDER BY cost ASC NULLS FIRST) AS cost_percent_rank,
        PERCENT_RANK() OVER (PARTITION BY order_id ORDER BY last7_count_amount DESC) AS last7_count_amount_percent_rank,
        NTILE(4) OVER (PARTITION BY order_id ORDER BY cost DESC, id ASC) AS cost_bucket,
        MIN(last7_sum_amount) OVER (PARTITION BY DATE_TRUNC('week', shipped_date), order_id) AS shipped_date_period_min_last7_sum_amount
    FROM with_windows
),
enriched_shipments AS (
    SELECT
        *,
        LEAD(last7_sum_amount) OVER (PARTITION BY order_id ORDER BY last7_min_amount ASC, id ASC) AS next_last7_sum_amount,
        LAG(last7_sum_amount) OVER (PARTITION BY order_id ORDER BY last7_min_amount ASC, id ASC) AS prev_last7_sum_amount,
        LEAD(last7_avg_amount) OVER (PARTITION BY order_id ORDER BY last7_min_amount ASC, id ASC) AS next_last7_avg_amount,
        LAG(last7_avg_amount) OVER (PARTITION BY order_id ORDER BY last7_min_amount ASC, id ASC) AS prev_last7_avg_amount,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY last7_min_amount ASC, id ASC) AS row_seq,
        SUM(shipped_date_period_min_last7_sum_amount) OVER (ORDER BY cost_percent_rank ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_shipped_date_period_min_last7_sum_amount
    FROM enriched
)
SELECT
    id,
    order_id,
    cost,
    last7_min_amount,
    last7_count_amount,
    last7_avg_amount,
    cost_percent_rank,
    last7_count_amount_percent_rank,
    cost_bucket
FROM enriched_shipments
