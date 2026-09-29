WITH valid AS (
    SELECT
        id,
        order_id,
        carrier,
        delivered_date,
        cost,
        status,
        ADD_MONTHS(delivered_date, 1) AS delivered_date_plus_months,
        COALESCE(carrier, 'N/A') AS carrier_filled
    FROM shipments
    WHERE shipped_date >= TO_DATE('2023-10-05')
      AND carrier IS NOT NULL
),
metrics AS (
    SELECT
        id,
        order_id,
        carrier,
        delivered_date,
        cost,
        status,
        delivered_date_plus_months,
        carrier_filled,
        NTILE(3) OVER (PARTITION BY carrier ORDER BY cost DESC, id) AS cost_bucket,
        CUME_DIST() OVER (PARTITION BY carrier ORDER BY cost DESC NULLS FIRST) AS cost_cume_dist,
        LAST_VALUE(cost) IGNORE NULLS OVER (PARTITION BY order_id ORDER BY delivered_date_plus_months, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_cost
    FROM valid
),
latest AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY delivered_date_plus_months DESC NULLS LAST) AS rank_in_group
        FROM metrics
    ) x
    WHERE rank_in_group = 1
),
metrics_shipments AS (
    SELECT
        id,
        order_id,
        carrier,
        delivered_date,
        cost,
        status,
        delivered_date_plus_months,
        carrier_filled,
        cost_bucket,
        cost_cume_dist,
        last_known_cost,
        rank_in_group,
        AVG(cost_cume_dist) OVER (PARTITION BY order_id ORDER BY delivered_date_plus_months, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS cost_cume_dist_avg_6,
        ROUND(cost / SUM(cost) OVER (PARTITION BY status), 4) AS cost_share,
        ROUND(last_known_cost / SUM(last_known_cost) OVER (PARTITION BY carrier), 4) AS last_known_cost_share
    FROM latest
),
enriched AS (
    SELECT
        e.*,
        b.discount,
        b.order_date
    FROM metrics_shipments e
    LEFT JOIN orders b
        ON e.order_id = b.id
),
matched_keys AS (
    SELECT order_id, status
    FROM enriched
    INTERSECT
    SELECT order_id, status
    FROM shipments
    WHERE shipped_date < '2023-04-23'
),
verified AS (
    SELECT s.*
    FROM enriched s
    INNER JOIN matched_keys k ON s.order_id = k.order_id AND s.status = k.status
),
filtered AS (
    SELECT
        id,
        order_id,
        carrier,
        cost,
        delivered_date_plus_months,
        carrier_filled,
        cost_bucket,
        cost_cume_dist,
        last_known_cost,
        rank_in_group,
        cost_cume_dist_avg_6,
        cost_share,
        last_known_cost_share,
        discount,
        CAST(discount AS INT) AS discount_int,
        discount + cost_share AS discount_plus_cost_share,
        COALESCE(carrier, 'UNKNOWN') AS resolved_carrier
    FROM verified
    WHERE carrier IN ('DHL', 'FedEx', 'Flash')
)
SELECT
    id,
    carrier,
    cost,
    delivered_date_plus_months,
    carrier_filled,
    cost_bucket,
    cost_cume_dist,
    last_known_cost,
    rank_in_group
FROM filtered
ORDER BY delivered_date_plus_months DESC, id DESC
