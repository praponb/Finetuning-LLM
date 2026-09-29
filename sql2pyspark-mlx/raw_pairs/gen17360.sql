WITH deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM shipments
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status, order_id ORDER BY delivered_date, id) = 1
),
deduped_shipments AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY shipped_date DESC, id) AS row_num
    FROM deduped
    QUALIFY row_num = 1
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        e.*,
        p.order_date,
        p.channel,
        p.discount
    FROM deduped_shipments e
    LEFT JOIN orders p
        ON e.order_id = p.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY carrier ORDER BY cost, id) AS cost_bucket,
        AVG(cost) OVER (ORDER BY row_num, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS cost_avg_2
    FROM with_dims
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    carrier,
    metric,
    val
FROM (
    SELECT
        id,
        carrier,
        discount,
        cost
    FROM metrics
) src
UNPIVOT (
    val FOR metric IN (discount AS DISCOUNT_VAL, cost AS COST_VAL)
);
