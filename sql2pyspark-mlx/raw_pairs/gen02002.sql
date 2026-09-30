WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        COALESCE(carrier, 'N/A') AS carrier_filled,
        DATE_TRUNC('YEAR', delivered_date) AS delivered_date_period
    FROM shipments
    WHERE carrier IS NOT NULL
      AND carrier IS NOT DISTINCT FROM 'FedEx'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        shipped_date,
        status,
        MAX(cost) OVER w_running AS last3_max_cost,
        MIN(cost) OVER w_running AS last3_min_cost,
        COUNT(cost) OVER w_running AS last3_count_cost,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY shipped_date DESC, id) AS event_seq
    FROM filtered
    WINDOW w_running AS (PARTITION BY carrier ORDER BY delivered_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Attach orders attributes
    SELECT
        s.id,
        s.order_id,
        s.last3_max_cost,
        s.last3_min_cost,
        s.last3_count_cost,
        s.event_seq,
        p.discount
    FROM metrics s
    INNER JOIN orders p
        ON s.order_id = p.id
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        order_id,
        last3_max_cost,
        last3_count_cost,
        event_seq,
        discount
    FROM enriched
    QUALIFY RANK() OVER (PARTITION BY order_id ORDER BY discount DESC NULLS LAST) = 1
)
SELECT
    id,
    last3_max_cost,
    last3_count_cost,
    discount
FROM ranked
