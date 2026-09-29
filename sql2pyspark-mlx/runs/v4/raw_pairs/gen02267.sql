WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        SUBSTRING(status, 1, 3) AS status_prefix,
        COALESCE(carrier, 'N/A') AS resolved_carrier
    FROM shipments
    WHERE order_id IS NOT NULL
      AND shipped_date <= '2023-10-01'
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LENGTH(carrier) AS carrier_length,
        COALESCE(carrier, 'N/A') AS carrier_clean
    FROM valid
    WHERE order_id IN (14, 27, 29)
),
joined AS (
    -- Attach orders attributes
    SELECT e.*, o.amount
    FROM filtered e
    LEFT JOIN orders o
        ON e.order_id = o.id
)
SELECT
    id,
    order_id,
    shipped_date
FROM joined;
