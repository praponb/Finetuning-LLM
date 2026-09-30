WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN status IN ('RETURNED', 'DELIVERED') THEN 'Group A' ELSE 'Other' END AS status_group,
        COALESCE(carrier, 'DIRECT') AS carrier_clean
    FROM shipments
    WHERE cost <= 84
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY cost ASC, id ASC) AS seq_no,
        CUME_DIST() OVER (PARTITION BY order_id ORDER BY cost ASC NULLS LAST) AS cost_cume_dist,
        DENSE_RANK() OVER (PARTITION BY status ORDER BY cost DESC) AS cost_dense_rank
    FROM filtered
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        status_group,
        carrier_clean,
        seq_no,
        cost_cume_dist,
        cost_dense_rank,
        FIRST_VALUE(cost_dense_rank, TRUE) OVER (PARTITION BY carrier ORDER BY cost_dense_rank ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cost_dense_rank,
        MAX(cost_dense_rank) OVER (PARTITION BY carrier, order_id) AS carrier_max_cost_dense_rank
    FROM scored
),
metrics_shipments AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        status_group,
        carrier_clean,
        seq_no,
        cost_cume_dist,
        cost_dense_rank,
        first_known_cost_dense_rank,
        carrier_max_cost_dense_rank,
        SUM(cost_cume_dist) OVER (PARTITION BY status ORDER BY shipped_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cost_cume_dist,
        ROW_NUMBER() OVER (ORDER BY cost_dense_rank ASC, id ASC) AS seq_no_2,
        LAG(carrier_max_cost_dense_rank, 1) OVER (PARTITION BY status ORDER BY first_known_cost_dense_rank ASC, id ASC) AS prev_carrier_max_cost_dense_rank
    FROM metrics
),
scored_shipments AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(prev_carrier_max_cost_dense_rank, 2) OVER w_part AS next_prev_carrier_max_cost_dense_rank,
        LAST_VALUE(first_known_cost_dense_rank) IGNORE NULLS OVER (PARTITION BY order_id, status ORDER BY seq_no_2 ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_first_known_cost_dense_rank,
        AVG(seq_no) OVER (PARTITION BY order_id ORDER BY seq_no_2 ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS seq_no_avg_2,
        MIN(cost_dense_rank) OVER (PARTITION BY carrier, order_id) AS carrier_min_cost_dense_rank,
        ROUND(first_known_cost_dense_rank / SUM(first_known_cost_dense_rank) OVER (PARTITION BY carrier, order_id), 4) AS first_known_cost_dense_rank_share
    FROM metrics_shipments
    WINDOW w_part AS (PARTITION BY order_id ORDER BY seq_no_2 ASC, id ASC)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY prev_carrier_max_cost_dense_rank DESC NULLS LAST, id ASC) AS dedup_rank
    FROM scored_shipments
    QUALIFY dedup_rank = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        status_group,
        carrier_clean,
        seq_no,
        cost_cume_dist,
        cost_dense_rank,
        first_known_cost_dense_rank,
        carrier_max_cost_dense_rank,
        cumulative_cost_cume_dist,
        seq_no_2,
        prev_carrier_max_cost_dense_rank,
        next_prev_carrier_max_cost_dense_rank,
        last_known_first_known_cost_dense_rank,
        seq_no_avg_2,
        carrier_min_cost_dense_rank,
        first_known_cost_dense_rank_share,
        dedup_rank,
        NTILE(5) OVER (ORDER BY prev_carrier_max_cost_dense_rank DESC, id ASC) AS prev_carrier_max_cost_dense_rank_bucket,
        LEAD(carrier_max_cost_dense_rank, 2) OVER (PARTITION BY order_id, carrier ORDER BY prev_carrier_max_cost_dense_rank ASC, id ASC) AS next_carrier_max_cost_dense_rank
    FROM deduped
)
SELECT
    id,
    order_id,
    carrier,
    shipped_date,
    delivered_date,
    status,
    status_group,
    carrier_clean,
    seq_no
FROM enriched
ORDER BY order_id DESC;
