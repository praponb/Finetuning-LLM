WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        carrier,
        shipped_date,
        cost,
        status,
        CONCAT_WS('/', carrier, status) AS carrier_status_key,
        COALESCE(carrier, 'NONE') AS carrier_filled
    FROM shipments
    WHERE order_id = 7
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        shipped_date,
        status,
        carrier_status_key,
        AVG(cost) OVER w_grp AS running_avg_cost,
        MIN(cost) OVER w_grp AS running_min_cost,
        COUNT(cost) OVER w_grp AS running_count_cost,
        SUM(cost) OVER w_grp AS running_sum_cost
    FROM filtered
    WHERE status = 'IN_TRANSIT'
    WINDOW w_grp AS (PARTITION BY carrier ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY running_min_cost DESC NULLS FIRST, id) = 1
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM deduped
    INTERSECT
    SELECT id
    FROM shipments
    WHERE status = 'LOST'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    INNER JOIN matched_keys k ON s.id = k.id
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        shipped_date,
        carrier_status_key,
        running_avg_cost,
        running_min_cost,
        running_count_cost,
        running_sum_cost,
        TRIM(status) AS clean_status
    FROM reconciled
    WHERE shipped_date > DATE '2023-04-24'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        running_avg_cost,
        running_count_cost,
        DENSE_RANK() OVER (ORDER BY running_min_cost NULLS LAST) AS running_min_cost_dense_rank,
        RANK() OVER (ORDER BY running_sum_cost DESC NULLS LAST) AS running_sum_cost_rank,
        DENSE_RANK() OVER (ORDER BY running_min_cost DESC) AS running_min_cost_dense_rank_2,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', shipped_date) ORDER BY running_count_cost DESC NULLS FIRST) AS running_count_cost_rank,
        RANK() OVER (ORDER BY running_count_cost DESC NULLS LAST) AS running_count_cost_rank_2
    FROM base
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY running_avg_cost DESC NULLS LAST) AS running_avg_cost_cume_dist,
        RANK() OVER (ORDER BY running_avg_cost NULLS LAST) AS running_avg_cost_rank,
        RANK() OVER (ORDER BY running_count_cost_rank_2 DESC NULLS FIRST) AS running_count_cost_rank_2_rank
    FROM enriched
)
SELECT
    running_avg_cost,
    running_count_cost,
    running_sum_cost_rank,
    running_min_cost_dense_rank_2,
    running_count_cost_rank,
    running_count_cost_rank_2,
    running_avg_cost_cume_dist,
    running_avg_cost_rank,
    running_count_cost_rank_2_rank
FROM metrics
ORDER BY running_count_cost_rank_2 NULLS FIRST;
