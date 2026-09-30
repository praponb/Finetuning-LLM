WITH latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        order_id,
        carrier,
        cost,
        status
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, carrier ORDER BY cost DESC, id) AS rn
        FROM shipments
    ) t
    WHERE rn = 1
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        a.id,
        a.order_id,
        a.carrier,
        a.cost,
        b.discount
    FROM latest a
    INNER JOIN orders b
        ON a.order_id = b.id
    WHERE b.shipping_fee < 17
),
kept_keys AS (
    -- Set difference on keys
    SELECT carrier
    FROM with_dims
    EXCEPT
    SELECT carrier
    FROM shipments
    WHERE order_id IN (1, 18)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    INNER JOIN kept_keys k ON s.carrier = k.carrier
),
scored AS (
    -- Ranking and gap features
    SELECT
        order_id,
        cost,
        discount,
        PERCENT_RANK() OVER (PARTITION BY carrier ORDER BY discount NULLS FIRST) AS discount_percent_rank,
        COUNT(discount) OVER w_ordered AS order_id_count_discount,
        RANK() OVER (ORDER BY discount DESC) AS discount_rank
    FROM verified
    WINDOW w_ordered AS (PARTITION BY order_id)
),
enriched AS (
    -- Attach orders attributes
    SELECT
        s.*,
        p.amount,
        p.status,
        p.channel
    FROM scored s
    LEFT JOIN orders p
        ON s.order_id = p.id
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        order_id,
        discount,
        discount_percent_rank,
        discount_rank,
        amount,
        status,
        channel,
        ROUND(cost * 1.15, 2) AS cost_scaled,
        COALESCE(status, 'OTHER') AS resolved_status
    FROM enriched
    WHERE discount <= 0.33
),
scored_shipments AS (
    -- Window metrics over several different partitions
    SELECT
        order_id,
        discount,
        discount_percent_rank,
        discount_rank,
        amount,
        STDDEV_SAMP(discount) OVER w_recent AS status_stddev_samp_discount,
        ROUND(amount / SUM(amount) OVER (PARTITION BY status), 4) AS amount_share
    FROM base
    WINDOW w_recent AS (PARTITION BY status)
)
-- Join a dimension and aggregate
SELECT
    d.status,
    COUNT(*) AS cnt,
    ROUND(AVG(e.status_stddev_samp_discount), 2) AS avg_status_stddev_samp_discount,
    AVG(e.amount_share) AS avg_amount_share
FROM scored_shipments e
LEFT JOIN orders d ON e.order_id = d.id
GROUP BY d.status
