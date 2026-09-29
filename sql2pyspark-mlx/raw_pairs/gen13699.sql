WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        carrier,
        shipped_date,
        cost,
        status,
        CONCAT(status, '/', carrier) AS status_carrier,
        TRIM(status) AS clean_status,
        COALESCE(carrier, 'NONE') AS carrier_filled
    FROM shipments
    WHERE carrier IS NOT NULL
      AND carrier IS DISTINCT FROM 'UPS'
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        carrier,
        shipped_date,
        cost,
        status,
        status_carrier,
        clean_status,
        carrier_filled,
        INITCAP(TRIM(status)) AS status_title
    FROM valid
    WHERE YEAR(shipped_date) >= 2025
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id, status
    FROM filtered
    INTERSECT ALL
    SELECT id, status
    FROM shipments
    WHERE order_id = 30
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN matched_keys k ON s.id = k.id AND s.status = k.status
),
valid_shipments AS (
    -- Keep recent, valid rows
    SELECT *, CAST(cost AS INT) AS cost_int
    FROM verified
    WHERE shipped_date >= DATE '2023-07-30'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(cost) OVER w_grp AS status_sum_cost,
        ROUND(cost / SUM(cost) OVER (PARTITION BY status), 4) AS cost_share,
        RANK() OVER (PARTITION BY status ORDER BY cost_int) AS cost_int_rank,
        PERCENT_RANK() OVER w_hist AS cost_int_percent_rank
    FROM valid_shipments
    WINDOW w_grp AS (PARTITION BY status, carrier), w_hist AS (PARTITION BY status ORDER BY cost_int NULLS FIRST)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY status ORDER BY cost_int_rank DESC NULLS FIRST) AS cost_int_rank_dense_rank,
        ROUND(cost_int_rank / SUM(cost_int_rank) OVER (PARTITION BY status), 4) AS cost_int_rank_share,
        PERCENT_RANK() OVER (ORDER BY cost_int_percent_rank DESC NULLS FIRST) AS cost_int_percent_rank_percent_rank,
        RANK() OVER (PARTITION BY carrier ORDER BY cost DESC) AS cost_rank,
        RANK() OVER (PARTITION BY carrier ORDER BY cost_int DESC NULLS FIRST) AS cost_int_rank_2
    FROM metrics
    WHERE status IN ('RETURNED', 'LOST', 'IN_TRANSIT')
),
scored_shipments AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        shipped_date,
        status_carrier,
        clean_status,
        carrier_filled,
        status_title,
        cost_share,
        cost_int_rank_share,
        cost_int_percent_rank_percent_rank,
        cost_rank,
        cost_int_rank_2,
        DENSE_RANK() OVER (ORDER BY cost_rank DESC NULLS LAST) AS cost_rank_dense_rank,
        MAX(cost_int_rank_share) OVER (PARTITION BY YEAR(shipped_date)) AS shipped_date_year_max_cost_int_rank_share,
        DENSE_RANK() OVER (ORDER BY cost_int DESC NULLS LAST) AS cost_int_dense_rank
    FROM scored
),
totals AS (
    -- Grouped metrics
    SELECT
        CASE WHEN cost_int_dense_rank >= 8 THEN 'LARGE' ELSE 'SMALL' END AS cost_int_dense_rank_tier,
        DATE_TRUNC('MONTH', shipped_date) AS activity_month,
        COUNT(*) AS num_rows,
        COUNT(DISTINCT id) AS distinct_ids
    FROM scored_shipments
    GROUP BY CASE WHEN cost_int_dense_rank >= 8 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('MONTH', shipped_date)
    HAVING distinct_ids > 1
)
SELECT
    cost_int_dense_rank_tier,
    activity_month,
    num_rows
FROM totals
ORDER BY activity_month DESC NULLS FIRST;
