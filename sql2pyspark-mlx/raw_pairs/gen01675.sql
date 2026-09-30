WITH valid AS (
    SELECT
        *,
        ROUND(cost, 2) AS cost_rounded,
        DATE_TRUNC('month', delivered_date) AS delivered_date_period,
        COALESCE(carrier, 'OTHER') AS carrier_filled
    FROM shipments
    WHERE order_id IS NOT NULL
      AND shipped_date <= '2023-03-01'
),
joined AS (
    SELECT
        s.id,
        s.order_id,
        s.carrier,
        s.delivered_date,
        s.cost,
        s.cost_rounded,
        s.carrier_filled,
        p.status AS order_status
    FROM valid s
    LEFT JOIN orders p
        ON s.order_id = p.id
    WHERE p.store_id IN (23, 25, 34)
),
enriched AS (
    SELECT
        *,
        AVG(cost_rounded) OVER w_grp AS last4_avg_cost_rounded,
        MAX(cost_rounded) OVER w_grp AS last4_max_cost_rounded,
        COUNT(cost_rounded) OVER w_grp AS last4_count_cost_rounded,
        MIN(cost_rounded) OVER w_grp AS last4_min_cost_rounded,
        ROW_NUMBER() OVER w_recent AS row_seq
    FROM joined
    WINDOW w_grp AS (PARTITION BY order_id ORDER BY cost, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY carrier, order_id ORDER BY cost DESC, id)
),
base AS (
    SELECT
        *,
        CASE WHEN last4_avg_cost_rounded >= 66 THEN 'High' WHEN last4_avg_cost_rounded >= 38 THEN 'Medium' ELSE 'Low' END AS last4_avg_cost_rounded_band,
        CONCAT_WS('-', carrier, order_status) AS carrier_order_status_key,
        COALESCE(carrier, 'DIRECT') AS resolved_carrier
    FROM enriched
    WHERE order_id IS NOT NULL
      AND delivered_date > TO_DATE('2023-06-20')
),
with_dims AS (
    SELECT
        src.*,
        p.order_date,
        p.status
    FROM base src
    LEFT JOIN orders p
        ON src.order_id <=> p.id
),
deduped AS (
    SELECT
        id,
        carrier,
        cost_rounded,
        carrier_filled,
        order_status,
        last4_max_cost_rounded,
        last4_count_cost_rounded,
        row_seq,
        carrier_order_status_key,
        resolved_carrier,
        order_date
    FROM with_dims
    QUALIFY ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY row_seq, id) = 1
)
SELECT
    id,
    carrier,
    carrier_filled,
    order_status,
    last4_max_cost_rounded,
    last4_count_cost_rounded,
    row_seq,
    carrier_order_status_key,
    resolved_carrier
FROM deduped
ORDER BY carrier DESC NULLS LAST, row_seq
