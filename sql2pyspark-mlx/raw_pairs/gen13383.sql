WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        shipped_date,
        delivered_date,
        cost,
        status,
        PERCENT_RANK() OVER (PARTITION BY order_id ORDER BY cost ASC NULLS FIRST) AS cost_percent_rank,
        SUM(cost) OVER (PARTITION BY status, carrier ORDER BY cost ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cost,
        SUM(cost) OVER (ORDER BY shipped_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost
    FROM shipments
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(cost_percent_rank) OVER (PARTITION BY status ORDER BY cost_percent_rank ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost_percent_rank,
        LAG(running_cost, 1) OVER (ORDER BY cost_percent_rank ASC, id ASC) AS prev_running_cost,
        SUM(cost) OVER (PARTITION BY status) AS status_sum_cost,
        NTILE(4) OVER (PARTITION BY status ORDER BY cumulative_cost ASC, id ASC) AS cumulative_cost_bucket
    FROM enriched
),
with_windows_shipments AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        shipped_date,
        delivered_date,
        cost,
        status,
        cost_percent_rank,
        cumulative_cost,
        prev_running_cost,
        status_sum_cost,
        cumulative_cost_bucket,
        MIN(running_cost_percent_rank) OVER (PARTITION BY status) AS status_min_running_cost_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('month', shipped_date), status ORDER BY cost_percent_rank ASC, id ASC) AS row_seq,
        MAX(cumulative_cost_bucket) OVER (PARTITION BY YEAR(shipped_date), status) AS shipped_date_year_max_cumulative_cost_bucket,
        NTILE(3) OVER (PARTITION BY status ORDER BY cumulative_cost_bucket DESC, id ASC) AS cumulative_cost_bucket_bucket,
        NTILE(10) OVER (PARTITION BY status ORDER BY prev_running_cost ASC, id ASC) AS prev_running_cost_bucket
    FROM with_windows
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM with_windows_shipments
    EXCEPT DISTINCT
    SELECT id
    FROM shipments
    WHERE YEAR(shipped_date) >= 2025
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows_shipments s
    INNER JOIN matched_keys k ON s.id = k.id
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        row_seq / NULLIF(cost_percent_rank, 0) AS row_seq_per_cost_percent_rank,
        GREATEST(shipped_date_year_max_cumulative_cost_bucket, status_sum_cost) AS greatest_shipped_date_year_max_cumulative_cost_bucket_status_sum_cost
    FROM reconciled
    WHERE shipped_date > '2023-04-06'
)
SELECT
    id,
    shipped_date,
    delivered_date,
    cost,
    status,
    cost_percent_rank,
    cumulative_cost,
    cumulative_cost_bucket,
    shipped_date_year_max_cumulative_cost_bucket,
    CASE WHEN status RLIKE '^(RETURNED|IN_TRANSIT)$' THEN 'GROUP_A' ELSE 'OTHER_ZONE' END AS status_zone
FROM cleaned
ORDER BY status_zone DESC NULLS FIRST;
