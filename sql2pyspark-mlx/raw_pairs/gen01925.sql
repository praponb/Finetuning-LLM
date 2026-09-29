WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        DATE_TRUNC('week', order_date) AS order_date_period,
        CONCAT_WS('-', channel, status) AS channel_status_key,
        COALESCE(channel, 'UNKNOWN') AS channel_clean
    FROM orders
    WHERE order_date > DATE '2023-03-13'
      AND amount BETWEEN 440 AND 560
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        amount,
        discount,
        order_date_period,
        channel_clean,
        ROW_NUMBER() OVER (PARTITION BY store_id, channel ORDER BY discount, id) AS seq_no,
        ROUND(amount / SUM(amount) OVER (PARTITION BY customer_id), 4) AS amount_share,
        CORR(discount, amount) OVER (PARTITION BY store_id) AS corr_discount_amount
    FROM base
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        customer_id,
        order_date,
        status,
        order_date_period,
        channel_clean,
        seq_no,
        amount_share,
        corr_discount_amount
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY seq_no, id) AS latest_rank
        FROM enriched
    ) x
    WHERE latest_rank = 1
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY customer_id ORDER BY seq_no DESC) AS row_num
    FROM ranked
    QUALIFY row_num = 1
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN order_date_period - LAG(order_date_period) OVER (PARTITION BY customer_id ORDER BY order_date_period, id) > INTERVAL 7 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM deduped
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY customer_id ORDER BY order_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
stats AS (
    -- Grouped metrics
    SELECT
        island_id,
        DATE_TRUNC('YEAR', order_date_period) AS year_start,
        SORT_ARRAY(COLLECT_SET(status)) AS status_list,
        COUNT(*) AS event_count,
        COUNT(*) AS cnt
    FROM islands
    GROUP BY ROLLUP (island_id, DATE_TRUNC('YEAR', order_date_period))
)
SELECT
    island_id,
    year_start,
    status_list,
    cnt
FROM stats
ORDER BY year_start NULLS LAST, cnt;
