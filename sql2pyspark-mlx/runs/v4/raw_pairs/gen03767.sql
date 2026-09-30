WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN status = 'LOST' THEN 1 ELSE 0 END AS is_lost,
        COALESCE(carrier, 'NONE') AS resolved_carrier
    FROM shipments
    WHERE carrier IS NOT NULL
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        STDDEV_SAMP(cost) OVER (PARTITION BY carrier) AS carrier_stddev_samp_cost,
        DENSE_RANK() OVER (PARTITION BY carrier ORDER BY cost DESC NULLS FIRST) AS cost_dense_rank
    FROM cleaned
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, status
    FROM metrics
    EXCEPT DISTINCT
    SELECT id, status
    FROM shipments
    WHERE shipped_date >= '2024-05-01'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.status = k.status
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(carrier_stddev_samp_cost) OVER (PARTITION BY order_id, status) AS order_id_sum_carrier_stddev_samp_cost,
        CUME_DIST() OVER (PARTITION BY status ORDER BY is_lost DESC NULLS LAST) AS is_lost_cume_dist,
        STDDEV_SAMP(is_lost) OVER (PARTITION BY status) AS status_stddev_samp_is_lost,
        ROUND(cost_dense_rank / SUM(cost_dense_rank) OVER (PARTITION BY status), 4) AS cost_dense_rank_share
    FROM kept_rows
)
-- Ranking and gap features
SELECT
    id,
    order_id,
    carrier,
    shipped_date,
    delivered_date,
    cost,
    status,
    is_lost,
    resolved_carrier,
    carrier_stddev_samp_cost,
    cost_dense_rank,
    order_id_sum_carrier_stddev_samp_cost,
    is_lost_cume_dist,
    status_stddev_samp_is_lost,
    cost_dense_rank_share,
    RANK() OVER w_running AS status_stddev_samp_is_lost_rank,
    MIN(cost_dense_rank) OVER w_running_2 AS order_id_min_cost_dense_rank,
    ROUND(cost_dense_rank / SUM(cost_dense_rank) OVER (PARTITION BY status), 4) AS cost_dense_rank_share_2,
    STDDEV_SAMP(carrier_stddev_samp_cost) OVER w_ordered AS status_stddev_samp_carrier_stddev_samp_cost
FROM scored
WINDOW w_running AS (ORDER BY status_stddev_samp_is_lost DESC NULLS LAST), w_running_2 AS (PARTITION BY order_id), w_ordered AS (PARTITION BY status);
