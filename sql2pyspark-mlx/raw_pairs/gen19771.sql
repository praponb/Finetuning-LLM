WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        status,
        HOUR(paid_at) AS paid_at_hour,
        SUBSTRING(method, 1, 3) AS method_prefix,
        COALESCE(method, 'UNKNOWN') AS resolved_method
    FROM payments
    WHERE amount > 620
      AND paid_at > TIMESTAMP '2023-11-14 19:00:00'
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        status,
        paid_at_hour,
        resolved_method,
        CEIL(paid_at_hour) AS paid_at_hour_whole,
        LENGTH(status) AS status_length
    FROM cleaned
    WHERE status = 'REFUNDED'
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY paid_at_hour, id) = 1
),
enriched AS (
    -- Attach orders attributes
    SELECT
        e.*,
        d.channel,
        d.order_date,
        d.amount
    FROM ranked e
    LEFT JOIN orders d
        ON e.order_id = d.id
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_date DESC NULLS LAST, id) AS row_num
        FROM enriched
    ) t
    WHERE row_num = 1
)
SELECT
    id,
    paid_at_hour,
    resolved_method,
    paid_at_hour_whole,
    order_date,
    amount,
    row_num
FROM deduped;
