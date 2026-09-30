WITH remaining_keys AS (
    -- Set intersection on keys
    SELECT order_id
    FROM shipments
    INTERSECT
    SELECT order_id
    FROM shipments
    WHERE carrier IN ('UPS', 'FedEx', 'Kerry')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM shipments s
    INNER JOIN remaining_keys k ON s.order_id = k.order_id
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        shipped_date,
        delivered_date,
        RANK() OVER w_grp AS cost_rank,
        MIN(cost) OVER w_running AS carrier_min_cost,
        MIN(cost) OVER w_running AS carrier_min_cost_2
    FROM kept_rows
    WINDOW w_grp AS (ORDER BY cost), w_running AS (PARTITION BY carrier)
),
base AS (
    -- Keep recent, valid rows
    SELECT *, CAST(cost_rank AS DOUBLE) AS cost_rank_num
    FROM enriched
    WHERE delivered_date BETWEEN '2023-08-01' AND '2025-01-01'
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        shipped_date,
        delivered_date,
        cost_rank,
        carrier_min_cost_2,
        cost_rank_num,
        CASE WHEN cost_rank >= 9 THEN 'High' WHEN cost_rank >= 4 THEN 'Medium' ELSE 'Low' END AS cost_rank_band,
        CASE WHEN carrier_min_cost >= 92 THEN 'High' WHEN carrier_min_cost >= 24 THEN 'Medium' ELSE 'Low' END AS carrier_min_cost_level
    FROM base
    WHERE delivered_date >= DATE '2023-09-05'
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(cost_rank_num AS INT) AS cost_rank_num_int,
        WEEKOFYEAR(shipped_date) AS shipped_date_weekofyear
    FROM filtered
    WHERE carrier_min_cost_2 IS NOT NULL
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('WEEK', delivered_date) AS week_start,
    DATE_TRUNC('YEAR', delivered_date) AS year_start,
    AVG(cost_rank_num_int) AS avg_cost_rank_num_int,
    ROUND(AVG(cost_rank_num), 2) AS avg_cost_rank_num,
    ROUND(AVG(shipped_date_weekofyear), 2) AS avg_shipped_date_weekofyear,
    SUM(cost_rank) AS total_cost_rank,
    COUNT(*) AS num_rows,
    COUNT(*) AS event_count
FROM valid
GROUP BY DATE_TRUNC('WEEK', delivered_date), DATE_TRUNC('YEAR', delivered_date);
