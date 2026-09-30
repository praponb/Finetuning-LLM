WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN carrier = 'UPS' THEN 1 ELSE 0 END AS is_ups,
        COALESCE(carrier, 'N/A') AS carrier_filled,
        COALESCE(carrier, 'NONE') AS carrier_filled_2
    FROM shipments
    WHERE carrier IS DISTINCT FROM 'FedEx'
      AND status <> 'RETURNED'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        shipped_date,
        delivered_date,
        cost,
        status,
        carrier_filled,
        carrier_filled_2,
        LAST_VALUE(cost, TRUE) OVER (PARTITION BY order_id ORDER BY is_ups, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_cost,
        DENSE_RANK() OVER (PARTITION BY status, order_id ORDER BY is_ups DESC NULLS LAST) AS is_ups_dense_rank,
        LEAD(is_ups, 2) OVER (PARTITION BY status ORDER BY cost, id) AS next_is_ups,
        AVG(cost) OVER (ORDER BY is_ups, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS cost_avg_2_prior
    FROM base
    WHERE status IN ('DELIVERED', 'LOST', 'IN_TRANSIT')
),
kept_keys AS (
    -- Set difference on keys
    SELECT status, id
    FROM enriched
    EXCEPT
    SELECT status, id
    FROM shipments
    WHERE MONTH(delivered_date) IN (1, 10, 11)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    JOIN kept_keys k ON s.status = k.status AND s.id = k.id
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM reconciled
    EXCEPT ALL
    SELECT id
    FROM shipments
    WHERE delivered_date BETWEEN '2024-05-20' AND '2025-05-11'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM reconciled s
    JOIN matched_keys k ON s.id = k.id
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        shipped_date,
        delivered_date,
        cost,
        status,
        carrier_filled,
        carrier_filled_2,
        last_known_cost,
        is_ups_dense_rank,
        next_is_ups,
        cost - cost_avg_2_prior AS cost_minus_cost_avg_2_prior
    FROM kept_rows
    WHERE shipped_date > '2024-05-01'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (PARTITION BY status ORDER BY next_is_ups DESC NULLS FIRST) AS next_is_ups_rank,
        CUME_DIST() OVER (PARTITION BY status ORDER BY cost_minus_cost_avg_2_prior DESC NULLS LAST) AS cost_minus_cost_avg_2_prior_cume_dist
    FROM valid
),
with_windows_shipments AS (
    -- Ranking and gap features
    SELECT
        shipped_date,
        delivered_date,
        status,
        carrier_filled,
        carrier_filled_2,
        last_known_cost,
        is_ups_dense_rank,
        cost_minus_cost_avg_2_prior,
        cost_minus_cost_avg_2_prior_cume_dist,
        MAX(next_is_ups_rank) OVER (PARTITION BY status) AS status_max_next_is_ups_rank,
        MAX(is_ups_dense_rank) OVER (PARTITION BY status) AS status_max_is_ups_dense_rank
    FROM with_windows
)
-- Grouped metrics
SELECT
    CASE WHEN status_max_next_is_ups_rank >= 7 THEN 'LARGE' ELSE 'SMALL' END AS status_max_next_is_ups_rank_tier,
    status,
    APPROX_PERCENTILE(status_max_is_ups_dense_rank, ARRAY(0.25, 0.5, 0.75)) AS status_max_is_ups_dense_rank_quartiles,
    SIZE(COLLECT_SET(status)) AS num_status,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN status_max_next_is_ups_rank >= 7 THEN 'LARGE' ELSE 'SMALL' END ORDER BY SIZE(COLLECT_SET(status)) DESC) AS num_status_percent_rank
FROM with_windows_shipments
GROUP BY 1, 2
HAVING num_status > 1
ORDER BY num_status DESC NULLS LAST
