WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        CEIL(cost) AS cost_whole
    FROM shipments
    WHERE delivered_date > DATE '2024-05-02'
      AND order_id IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(cost_whole) OVER w_hist AS status_sum_cost_whole,
        AVG(cost) OVER (PARTITION BY carrier, status ORDER BY delivered_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS cost_avg_3,
        NTILE(10) OVER (ORDER BY cost_whole DESC, id) AS cost_whole_bucket
    FROM cleaned
    WINDOW w_hist AS (PARTITION BY status)
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        cost_whole_bucket * cost AS cost_whole_bucket_times_cost,
        CASE WHEN cost >= 106 THEN 'High' WHEN cost >= 42 THEN 'Medium' ELSE 'Low' END AS cost_tier
    FROM enriched
    WHERE delivered_date >= TO_DATE('2023-11-13')
      AND carrier IS NOT NULL
),
valid_shipments AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        cost * cost_whole AS cost_times_cost_whole,
        COALESCE(carrier, 'DIRECT') AS resolved_carrier
    FROM valid
    WHERE carrier IS NOT NULL
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY delivered_date NULLS FIRST, id) AS rank_in_group
    FROM valid_shipments
    QUALIFY rank_in_group = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(cost_avg_3) OVER w_grp AS running_cost_avg_3,
        NTILE(5) OVER w_grp_2 AS cost_times_cost_whole_bucket,
        SUM(cost_times_cost_whole) OVER (ORDER BY cost_whole_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost_times_cost_whole,
        ROW_NUMBER() OVER w_recent AS seq_no,
        RANK() OVER (PARTITION BY status ORDER BY cost_whole_bucket) AS cost_whole_bucket_rank
    FROM ranked
    WINDOW w_grp AS (PARTITION BY status, carrier ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp_2 AS (PARTITION BY carrier ORDER BY cost_times_cost_whole DESC, id), w_recent AS (ORDER BY cost, id)
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY status_sum_cost_whole DESC, id) AS row_num
    FROM scored
    QUALIFY row_num = 1
)
SELECT
    id,
    carrier,
    delivered_date,
    status,
    cost_whole,
    status_sum_cost_whole,
    cost_avg_3,
    cost_whole_bucket,
    cost_whole_bucket_times_cost,
    CASE WHEN status RLIKE '^(LOST|DELIVERED)$' THEN 'EXTENDED' WHEN status RLIKE '^(IN_TRANSIT|RETURNED)$' THEN 'CORE' ELSE 'PRIORITY' END AS status_zone
FROM deduped
ORDER BY cost_whole_bucket_times_cost
