WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(cost) OVER (PARTITION BY MONTH(shipped_date), carrier) AS shipped_date_month_sum_cost,
        SUM(cost) OVER (ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost,
        ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY delivered_date, id) AS row_seq
    FROM shipments
),
matched_keys AS (
    -- Set difference on keys
    SELECT carrier
    FROM metrics
    EXCEPT ALL
    SELECT carrier
    FROM shipments
    WHERE status = 'DELIVERED'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    INNER JOIN matched_keys k ON s.carrier = k.carrier
),
enriched AS (
    -- Attach orders attributes
    SELECT
        e.id,
        e.order_id,
        e.shipped_date,
        e.delivered_date,
        e.cost,
        e.status,
        e.shipped_date_month_sum_cost,
        e.running_cost,
        e.row_seq,
        d.discount
    FROM kept_rows e
    LEFT JOIN orders d
        ON e.order_id = d.id
),
scored AS (
    -- Running and moving aggregates
    SELECT *, ROUND(shipped_date_month_sum_cost / SUM(shipped_date_month_sum_cost) OVER (PARTITION BY order_id), 4) AS shipped_date_month_sum_cost_share
    FROM enriched
),
scored_shipments AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY order_id, status ORDER BY shipped_date_month_sum_cost DESC NULLS LAST) AS shipped_date_month_sum_cost_percent_rank,
        RANK() OVER (PARTITION BY order_id ORDER BY row_seq DESC NULLS LAST) AS row_seq_rank,
        ROUND(shipped_date_month_sum_cost / SUM(shipped_date_month_sum_cost) OVER (PARTITION BY status, order_id), 4) AS shipped_date_month_sum_cost_share_2
    FROM scored
),
joined AS (
    -- Attach orders attributes
    SELECT
        a.*,
        b.status AS order_status,
        b.order_date
    FROM scored_shipments a
    LEFT JOIN orders b
        ON a.order_id = b.id
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        shipped_date,
        delivered_date,
        cost,
        status,
        shipped_date_month_sum_cost,
        running_cost,
        discount,
        shipped_date_month_sum_cost_share,
        shipped_date_month_sum_cost_percent_rank,
        row_seq_rank,
        order_status,
        order_date,
        CASE WHEN shipped_date_month_sum_cost >= 457 THEN 'High' WHEN shipped_date_month_sum_cost >= 193 THEN 'Medium' ELSE 'Low' END AS shipped_date_month_sum_cost_tier
    FROM joined
    WHERE delivered_date >= DATE '2023-04-25'
      AND order_status IS NOT NULL
),
summary AS (
    -- Multi-level aggregation
    SELECT
        status,
        SIZE(COLLECT_SET(status)) AS num_status,
        STDDEV_SAMP(cost) AS cost_stddev,
        SUM(shipped_date_month_sum_cost_percent_rank) AS total_shipped_date_month_sum_cost_percent_rank,
        APPROX_PERCENTILE(shipped_date_month_sum_cost_percent_rank, 0.5) AS shipped_date_month_sum_cost_percent_rank_p50,
        COUNT(CASE WHEN status = 'RETURNED' THEN 1 END) AS returned_count
    FROM filtered
    GROUP BY status
)
SELECT
    status,
    num_status,
    cost_stddev,
    returned_count,
    CASE WHEN status RLIKE '^(RETURNED|DELIVERED)$' THEN 'EXTENDED' ELSE 'OTHER_ZONE' END AS status_zone
FROM summary
ORDER BY returned_count NULLS FIRST;
