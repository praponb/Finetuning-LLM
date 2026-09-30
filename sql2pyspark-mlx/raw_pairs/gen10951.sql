WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        TRIM(carrier) AS clean_carrier,
        COALESCE(carrier, 'NONE') AS carrier_filled
    FROM shipments
    WHERE carrier IS NOT DISTINCT FROM 'DHL'
      AND order_id IS NULL
),
with_dims AS (
    -- Attach orders attributes
    SELECT a.*, o.amount
    FROM filtered a
    INNER JOIN orders o
        ON a.order_id = o.id
),
enriched AS (
    -- Attach orders attributes
    SELECT
        e.id,
        e.order_id,
        e.shipped_date,
        e.cost,
        e.status,
        o.shipping_fee
    FROM with_dims e
    INNER JOIN orders o
        ON e.order_id = o.id
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        shipped_date,
        cost,
        status,
        shipping_fee
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY shipped_date DESC, id) = 1
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        cost,
        shipping_fee,
        INITCAP(TRIM(status)) AS status_title
    FROM ranked
    WHERE shipped_date > DATE '2023-09-04'
),
cleaned_shipments AS (
    -- Keep recent, valid rows
    SELECT *, LEAST(cost, shipping_fee) AS least_cost_shipping_fee
    FROM cleaned
    WHERE cost < 68
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        shipping_fee,
        LAG(shipping_fee) OVER w_ordered AS prev_shipping_fee,
        LAG(cost) OVER w_ordered AS prev_cost,
        LEAD(cost) OVER w_ordered AS next_cost,
        ROW_NUMBER() OVER w_ordered AS seq_no
    FROM cleaned_shipments
    WINDOW w_ordered AS (ORDER BY shipping_fee, id)
)
-- Multi-level aggregation
SELECT
    CASE WHEN next_cost >= 87 THEN 'PREMIUM' ELSE 'STANDARD' END AS next_cost_tier,
    seq_no,
    ROUND(AVG(seq_no), 2) AS avg_seq_no,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENTILE_APPROX(prev_shipping_fee, ARRAY(0.25, 0.5, 0.75)) AS prev_shipping_fee_quartiles
FROM scored
GROUP BY GROUPING SETS ((CASE WHEN next_cost >= 87 THEN 'PREMIUM' ELSE 'STANDARD' END, seq_no), (CASE WHEN next_cost >= 87 THEN 'PREMIUM' ELSE 'STANDARD' END))
HAVING distinct_ids > 1
ORDER BY next_cost_tier DESC NULLS LAST;
