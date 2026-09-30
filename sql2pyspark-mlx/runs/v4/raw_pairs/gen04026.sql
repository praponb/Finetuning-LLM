WITH with_dims AS (
    SELECT
        src.*,
        b.status AS order_status,
        b.channel
    FROM shipments src
    LEFT JOIN orders b
        ON src.order_id = b.id
),
remaining_keys AS (
    SELECT id
    FROM with_dims
    EXCEPT ALL
    SELECT id
    FROM shipments
    WHERE carrier IN ('FedEx', 'UPS')
),
kept_rows AS (
    SELECT s.*
    FROM with_dims s
    JOIN remaining_keys k ON s.id = k.id
),
enriched AS (
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        order_status,
        channel,
        PERCENT_RANK() OVER (ORDER BY cost DESC NULLS FIRST) AS cost_percent_rank,
        ROUND(cost / SUM(cost) OVER (PARTITION BY channel), 4) AS cost_share
    FROM kept_rows
),
cleaned AS (
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        order_status,
        channel,
        cost_percent_rank,
        cost_share,
        ROUND(cost_percent_rank * 1.07, 2) AS cost_percent_rank_adjusted,
        COALESCE(channel, 'N/A') AS resolved_channel
    FROM enriched
    WHERE delivered_date >= DATE '2024-03-29'
),
scored AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY channel ORDER BY cost_percent_rank DESC NULLS LAST) AS cost_percent_rank_rank,
        CUME_DIST() OVER (PARTITION BY order_id ORDER BY cost_percent_rank DESC) AS cost_percent_rank_cume_dist,
        PERCENT_RANK() OVER (PARTITION BY order_status ORDER BY cost_share DESC NULLS FIRST) AS cost_share_percent_rank
    FROM cleaned
),
kept_keys AS (
    SELECT order_id, id
    FROM scored
    EXCEPT ALL
    SELECT order_id, id
    FROM shipments
    WHERE shipped_date < '2024-06-01'
),
kept_rows_shipments AS (
    SELECT s.*
    FROM scored s
    JOIN kept_keys k ON s.order_id = k.order_id AND s.id = k.id
),
kept_keys_2 AS (
    SELECT status, carrier
    FROM kept_rows_shipments
    INTERSECT
    SELECT status, carrier
    FROM shipments
    WHERE order_id IN (18, 23, 38)
),
verified AS (
    SELECT s.*
    FROM kept_rows_shipments s
    JOIN kept_keys_2 k ON s.status = k.status AND s.carrier = k.carrier
)
SELECT *
FROM (
    SELECT
        order_status,
        cost_percent_rank_adjusted,
        cost_share,
        cost_percent_rank_cume_dist
    FROM verified
) src
UNPIVOT (
    reading FOR metric_name IN (cost_percent_rank_adjusted AS COST_PERCENT_RANK_ADJUSTED, cost_share, cost_percent_rank_cume_dist AS COST_PERCENT_RANK_CUME_DIST_VAL)
);
