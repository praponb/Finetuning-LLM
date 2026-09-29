WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        status,
        ADD_MONTHS(shipped_date, 6) AS shipped_date_plus_months,
        COALESCE(carrier, 'N/A') AS carrier_clean
    FROM shipments
    WHERE delivered_date >= TO_DATE('2023-04-19')
      AND order_id IS NOT NULL
      AND order_id = 8
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY shipped_date_plus_months ASC, id ASC) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY shipped_date ASC, id ASC) AS event_seq
    FROM cleaned
),
kept_keys AS (
    -- Set difference on keys
    SELECT carrier
    FROM with_windows
    EXCEPT DISTINCT
    SELECT carrier
    FROM shipments
    WHERE order_id IN (1, 14)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    JOIN kept_keys k ON s.carrier = k.carrier
),
joined AS (
    -- Attach orders attributes
    SELECT a.*, p.order_date
    FROM kept_rows a
    LEFT JOIN orders p
        ON a.order_id = p.id
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        s.*,
        p.order_date AS order_order_date,
        p.discount
    FROM joined s
    JOIN orders p
        ON s.order_id = p.id
)
-- Multi-level aggregation
SELECT
    CASE WHEN row_seq >= 8 THEN 'HIGH' ELSE 'LOW' END AS row_seq_tier,
    ROUND(AVG(row_seq), 2) AS avg_row_seq,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    PERCENTILE_APPROX(discount, 0.5) AS discount_p50,
    MAX(event_seq) FILTER (WHERE status = 'IN_TRANSIT') AS in_transit_max_event_seq
FROM with_dims
GROUP BY row_seq_tier
HAVING avg_row_seq >= 0;
