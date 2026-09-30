WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY order_id, carrier ORDER BY cost NULLS FIRST) AS rn
    FROM shipments
    QUALIFY rn = 1
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        a.id,
        a.order_id,
        a.carrier,
        a.shipped_date,
        a.delivered_date,
        a.cost,
        a.status,
        a.rn,
        d.amount
    FROM ranked a
    LEFT JOIN orders d
        ON a.order_id = d.id
),
ranked_shipments AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier, order_id ORDER BY amount NULLS FIRST, id) AS rn_2
    FROM with_dims
    QUALIFY rn_2 = 1
),
enriched AS (
    -- Attach orders attributes
    SELECT s.*, p.order_date
    FROM ranked_shipments s
    LEFT JOIN orders p
        ON s.order_id = p.id
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        shipped_date,
        cost,
        status,
        amount,
        rn_2,
        order_date,
        CEIL(cost) AS cost_whole
    FROM enriched
    WHERE order_id IS NOT NULL
      AND delivered_date >= TO_DATE('2023-04-28')
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(cost / SUM(cost) OVER (PARTITION BY status), 4) AS cost_share,
        ROUND(amount / SUM(amount) OVER (PARTITION BY status), 4) AS amount_share,
        LAST_VALUE(amount, TRUE) OVER (PARTITION BY status ORDER BY cost_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_amount,
        ROUND(cost / SUM(cost) OVER (PARTITION BY status), 4) AS cost_share_2,
        SUM(cost) OVER (PARTITION BY MONTH(shipped_date), status ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost
    FROM base
)
-- Grouped metrics
SELECT
    CASE WHEN amount_share >= 0.73 THEN 'LARGE' ELSE 'SMALL' END AS amount_share_tier,
    SIZE(COLLECT_SET(status)) AS num_status,
    MAX(amount) FILTER (WHERE status = 'RETURNED') AS returned_max_amount,
    ROUND(AVG(cost_share_2), 2) AS avg_cost_share_2,
    APPROX_PERCENTILE(running_cost, 0.9) AS running_cost_p90
FROM scored
GROUP BY 1
