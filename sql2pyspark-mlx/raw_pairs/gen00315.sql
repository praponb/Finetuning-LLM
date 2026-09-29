WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        carrier,
        delivered_date,
        cost,
        status,
        ROUND(DATEDIFF(shipped_date, delivered_date) / 30, 1) AS months_delivered_date_to_shipped_date
    FROM shipments
    WHERE shipped_date >= DATE '2023-05-05'
      AND order_id IS NOT NULL
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM base
    EXCEPT ALL
    SELECT id
    FROM shipments
    WHERE shipped_date <= '2024-03-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    JOIN kept_keys k ON s.id = k.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        carrier,
        delivered_date,
        cost,
        status,
        months_delivered_date_to_shipped_date,
        CUME_DIST() OVER (PARTITION BY MONTH(delivered_date), carrier ORDER BY months_delivered_date_to_shipped_date) AS months_delivered_date_to_shipped_date_cume_dist,
        COUNT(months_delivered_date_to_shipped_date) OVER (PARTITION BY carrier, status) AS carrier_count_months_delivered_date_to_shipped_date,
        MIN(months_delivered_date_to_shipped_date) OVER (PARTITION BY status, carrier) AS status_min_months_delivered_date_to_shipped_date
    FROM verified
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        cost,
        months_delivered_date_to_shipped_date,
        CUME_DIST() OVER (PARTITION BY status ORDER BY status_min_months_delivered_date_to_shipped_date DESC NULLS FIRST) AS status_min_months_delivered_date_to_shipped_date_cume_dist
    FROM scored
    WHERE status_min_months_delivered_date_to_shipped_date >= 37
),
base_shipments AS (
    -- Basic cleansing and defaults
    SELECT *, status_min_months_delivered_date_to_shipped_date_cume_dist - cost AS status_min_months_delivered_date_to_shipped_date_cume_dist_minus_cost
    FROM enriched
    WHERE months_delivered_date_to_shipped_date < 53
),
scored_shipments AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY months_delivered_date_to_shipped_date NULLS LAST) AS months_delivered_date_to_shipped_date_cume_dist,
        CUME_DIST() OVER (ORDER BY status_min_months_delivered_date_to_shipped_date_cume_dist) AS status_min_months_delivered_date_to_shipped_date_cume_dist_cume_dist
    FROM base_shipments
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        months_delivered_date_to_shipped_date,
        status_min_months_delivered_date_to_shipped_date_cume_dist,
        status_min_months_delivered_date_to_shipped_date_cume_dist_minus_cost,
        CUME_DIST() OVER (ORDER BY months_delivered_date_to_shipped_date DESC) AS months_delivered_date_to_shipped_date_cume_dist_2,
        CUME_DIST() OVER (ORDER BY cost NULLS LAST) AS cost_cume_dist
    FROM scored_shipments
)
SELECT
    months_delivered_date_to_shipped_date,
    status_min_months_delivered_date_to_shipped_date_cume_dist,
    months_delivered_date_to_shipped_date_cume_dist_2,
    cost_cume_dist
FROM metrics
ORDER BY status_min_months_delivered_date_to_shipped_date_cume_dist DESC NULLS LAST, months_delivered_date_to_shipped_date;
