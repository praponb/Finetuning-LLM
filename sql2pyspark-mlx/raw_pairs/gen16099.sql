WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        TRIM(status) AS clean_status,
        COALESCE(channel, 'OTHER') AS resolved_channel
    FROM orders
    WHERE order_date >= TO_DATE('2023-09-09')
      AND customer_id = 14
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        CORR(discount, amount) OVER (PARTITION BY status) AS corr_discount_amount,
        RANK() OVER (PARTITION BY DATE_TRUNC('WEEK', order_date), customer_id ORDER BY amount DESC NULLS LAST) AS amount_rank,
        ROW_NUMBER() OVER (PARTITION BY customer_id, store_id ORDER BY amount DESC, id) AS event_seq,
        ROW_NUMBER() OVER (ORDER BY discount DESC, id) AS row_seq
    FROM cleaned
    WHERE status = 'SHIPPED'
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, INITCAP(TRIM(status)) AS status_title
    FROM metrics
    WHERE order_date >= TO_DATE('2023-10-02')
      AND channel IS DISTINCT FROM 'mobile'
),
joined AS (
    -- Attach customers attributes
    SELECT src.*, c.credit_limit
    FROM base src
    JOIN customers c
        ON src.customer_id = c.id
)
-- Grouped metrics
SELECT
    store_id,
    DATE_TRUNC('YEAR', order_date) AS year_start,
    APPROX_PERCENTILE(row_seq, 0.5) AS row_seq_p50,
    SUM(CASE WHEN channel = 'web' THEN row_seq ELSE 0 END) AS web_row_seq,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    COUNT(DISTINCT store_id) AS distinct_store_ids,
    COUNT(CASE WHEN channel = 'web' THEN 1 END) AS web_count
FROM joined
GROUP BY GROUPING SETS ((store_id, DATE_TRUNC('YEAR', order_date)), (store_id));
