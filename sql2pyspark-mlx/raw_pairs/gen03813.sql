WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        carrier,
        delivered_date,
        cost,
        status,
        DATE_TRUNC('week', shipped_date) AS shipped_date_period,
        COALESCE(carrier, 'OTHER') AS carrier_clean
    FROM shipments
    WHERE YEAR(delivered_date) = 2024
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        carrier,
        delivered_date,
        status,
        shipped_date_period,
        carrier_clean,
        AVG(cost) OVER (ORDER BY cost, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS cost_avg_6_prior,
        COUNT(cost) OVER (PARTITION BY status ORDER BY CAST(shipped_date_period AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS cost_count_30d
    FROM filtered
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.delivered_date,
        src.cost_avg_6_prior,
        d.order_date,
        d.channel
    FROM scored src
    JOIN orders d
        ON src.order_id = d.id
    WHERE d.store_id IS NULL
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        order_date,
        CASE WHEN channel = 'partner' THEN 1 ELSE 0 END AS is_partner
    FROM with_dims
    WHERE delivered_date >= DATE '2023-09-09'
)
-- Join a dimension and aggregate
SELECT
    dim.status,
    COUNT(*) AS cnt,
    MAX(a.is_partner) AS max_is_partner
FROM cleaned a
LEFT JOIN orders dim ON a.order_id = dim.id
GROUP BY dim.status;
