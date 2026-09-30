WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        CONCAT(carrier, ', ', status) AS carrier_status,
        COALESCE(carrier, 'N/A') AS resolved_carrier
    FROM shipments
    WHERE shipped_date >= DATE '2023-07-27'
      AND cost <= 54
),
valid_shipments AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        carrier,
        delivered_date,
        cost,
        carrier_status,
        COALESCE(cost, 0) AS cost_filled,
        COALESCE(carrier, 'UNKNOWN') AS resolved_carrier_2
    FROM valid
    WHERE delivered_date >= DATE '2023-10-03'
      AND order_id IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY YEAR(delivered_date), carrier ORDER BY cost_filled, id) AS cost_filled_bucket,
        SUM(cost) OVER (PARTITION BY order_id, carrier ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cost,
        FIRST_VALUE(cost, TRUE) OVER (PARTITION BY order_id ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cost,
        AVG(cost_filled) OVER (PARTITION BY carrier ORDER BY cost, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS cost_filled_avg_6_prior,
        AVG(cost) OVER (ORDER BY cost, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS cost_avg_6_prior
    FROM valid_shipments
),
joined AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.carrier,
        src.delivered_date,
        src.cumulative_cost,
        src.cost_filled_avg_6_prior,
        src.cost_avg_6_prior,
        d.channel,
        d.status,
        d.amount
    FROM enriched src
    LEFT JOIN orders d
        ON src.order_id = d.id
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY cost_filled_avg_6_prior DESC NULLS LAST, id) AS dedup_rank
    FROM joined
    QUALIFY dedup_rank = 1
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN cost_avg_6_prior >= 73 THEN 'LARGE' ELSE 'SMALL' END AS cost_avg_6_prior_tier,
        CASE WHEN cost_filled_avg_6_prior >= 71 THEN 'PREMIUM' ELSE 'STANDARD' END AS cost_filled_avg_6_prior_tier,
        SUM(CASE WHEN channel = 'partner' THEN cumulative_cost ELSE 0 END) AS partner_cumulative_cost,
        AVG(cost_avg_6_prior) AS avg_cost_avg_6_prior,
        ROUND(SUM(amount) / NULLIF(COUNT(CASE WHEN carrier = 'Kerry' THEN 1 END), 0), 2) AS amount_per_kerry,
        COUNT_IF(channel = 'partner') AS partner_events
    FROM ranked
    GROUP BY CASE WHEN cost_avg_6_prior >= 73 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN cost_filled_avg_6_prior >= 71 THEN 'PREMIUM' ELSE 'STANDARD' END
    HAVING partner_cumulative_cost > 0
)
SELECT
    cost_avg_6_prior_tier,
    cost_filled_avg_6_prior_tier,
    partner_cumulative_cost,
    avg_cost_avg_6_prior,
    amount_per_kerry,
    partner_events
FROM totals
ORDER BY partner_events DESC NULLS FIRST
