WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        QUARTER(paid_at) AS paid_at_quarter,
        CONCAT_WS('/', method, status) AS method_status_key
    FROM payments
    WHERE method IS DISTINCT FROM 'bank_transfer'
      AND order_id IS NOT NULL
),
enriched AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.amount,
        src.method_status_key,
        o.status AS order_status,
        o.discount,
        o.amount AS order_amount
    FROM valid src
    LEFT JOIN orders o
        ON src.order_id = o.id
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY discount DESC NULLS FIRST, id) AS dedup_rank
    FROM enriched
    QUALIFY dedup_rank = 1
),
latest_payments AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY dedup_rank, id) = 1
)
SELECT
    id,
    method_status_key,
    order_status,
    discount,
    CASE WHEN order_status RLIKE '^(COMPLETED|RETURNED)$' THEN 'GROUP_B' WHEN order_status RLIKE '^(PENDING|CANCELLED)$' THEN 'OTHER_ZONE' ELSE 'EXTENDED' END AS order_status_zone
FROM latest_payments;
