WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM shipments
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY delivered_date DESC, id) = 1
),
joined AS (
    -- Attach orders attributes
    SELECT
        a.id,
        a.order_id,
        a.shipped_date,
        a.status,
        o.status AS order_status
    FROM latest a
    LEFT JOIN orders o
        ON a.order_id = o.id
    WHERE o.order_date < '2024-08-01'
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        status,
        SUBSTRING(status, 1, 2) AS status_prefix
    FROM joined
    WHERE shipped_date > DATE '2023-02-15'
      AND order_id IS NOT NULL
),
base_shipments AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        UPPER(status) AS status_upper,
        INITCAP(TRIM(status)) AS status_title
    FROM base
    WHERE order_id IS NOT NULL
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, LOWER(status) AS status_lower
    FROM base_shipments
    WHERE status NOT IN ('IN_TRANSIT', 'DELIVERED')
)
SELECT
    id,
    status,
    status_upper
FROM valid;
