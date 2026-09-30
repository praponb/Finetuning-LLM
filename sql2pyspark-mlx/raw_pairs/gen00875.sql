WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATE_TRUNC('month', delivered_date) AS delivered_date_period,
        DATE_TRUNC('week', delivered_date) AS delivered_date_period_2,
        COALESCE(carrier, 'UNKNOWN') AS carrier_clean
    FROM shipments
    WHERE order_id IS NOT NULL
      AND carrier IS DISTINCT FROM 'Kerry'
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, status
    FROM cleaned
    EXCEPT
    SELECT id, status
    FROM shipments
    WHERE shipped_date > '2025-07-01'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN kept_keys k ON s.id = k.id AND s.status = k.status
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(cost) OVER (ORDER BY CAST(delivered_date_period AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS cost_count_7d,
        AVG(cost) OVER (PARTITION BY MONTH(delivered_date_period_2) ORDER BY CAST(delivered_date_period_2 AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS cost_avg_7d,
        MAX(cost) OVER (ORDER BY CAST(delivered_date_period AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS cost_max_7d,
        ROUND(cost / SUM(cost) OVER (PARTITION BY MONTH(delivered_date_period_2), status), 4) AS cost_share,
        MAX(cost) OVER (PARTITION BY status ORDER BY CAST(delivered_date_period_2 AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS cost_max_30d
    FROM kept_rows
),
metrics_shipments AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        delivered_date,
        delivered_date_period,
        delivered_date_period_2,
        carrier_clean,
        cost_avg_7d,
        cost_max_30d,
        SUM(cost_avg_7d) OVER w_ordered AS cost_avg_7d_sum_7d,
        SUM(cost_count_7d) OVER (PARTITION BY status, carrier ORDER BY CAST(delivered_date_period AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS cost_count_7d_sum_90d,
        COUNT(cost_avg_7d) OVER (PARTITION BY status ORDER BY CAST(delivered_date_period_2 AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS cost_avg_7d_count_7d
    FROM metrics
    WHERE cost_max_30d > 52
    WINDOW w_ordered AS (ORDER BY CAST(delivered_date_period AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(cost_count_7d_sum_90d) OVER (PARTITION BY order_id) AS order_id_sum_cost_count_7d_sum_90d,
        MAX(cost_max_30d) OVER (PARTITION BY order_id ORDER BY CAST(delivered_date_period_2 AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS cost_max_30d_max_30d
    FROM metrics_shipments
),
joined AS (
    -- Attach orders attributes
    SELECT
        src.*,
        b.channel,
        b.shipping_fee,
        b.amount
    FROM with_windows src
    LEFT JOIN orders b
        ON src.order_id = b.id
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(cost_count_7d_sum_90d / SUM(cost_count_7d_sum_90d) OVER (PARTITION BY YEAR(delivered_date)), 4) AS cost_count_7d_sum_90d_share,
        SUM(cost_avg_7d_count_7d) OVER (PARTITION BY YEAR(delivered_date_period_2)) AS delivered_date_period_2_year_sum_cost_avg_7d_count_7d,
        COUNT(cost_count_7d_sum_90d) OVER (PARTITION BY order_id, channel ORDER BY CAST(delivered_date_period AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS cost_count_7d_sum_90d_count_90d,
        SUM(cost_count_7d_sum_90d) OVER (PARTITION BY channel ORDER BY CAST(delivered_date_period AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS cost_count_7d_sum_90d_sum_7d,
        ROUND(cost_avg_7d_count_7d / SUM(cost_avg_7d_count_7d) OVER (PARTITION BY order_id), 4) AS cost_avg_7d_count_7d_share
    FROM joined
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('WEEK', delivered_date_period_2) AS week_start,
    MAX(cost_max_30d_max_30d) FILTER (WHERE channel = 'store') AS store_max_cost_max_30d_max_30d,
    ROUND(AVG(order_id_sum_cost_count_7d_sum_90d), 2) AS avg_order_id_sum_cost_count_7d_sum_90d
FROM enriched
GROUP BY week_start;
