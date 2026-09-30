WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ADD_MONTHS(delivered_date, 1) AS delivered_date_plus_months,
        DATE_ADD(shipped_date, 24) AS shipped_date_plus_24d,
        COALESCE(carrier, 'N/A') AS carrier_clean
    FROM shipments
    WHERE delivered_date >= DATE '2023-07-30'
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        shipped_date,
        delivered_date,
        cost,
        status,
        shipped_date_plus_24d,
        carrier_clean,
        SUBSTRING(carrier, 1, 4) AS carrier_prefix
    FROM base
    WHERE order_id = 39
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(cost) OVER w_grp AS last7_avg_cost,
        MAX(cost) OVER w_grp AS last7_max_cost,
        COUNT(cost) OVER w_grp AS last7_count_cost,
        SUM(cost) OVER w_grp AS last7_sum_cost,
        ROUND(cost / SUM(cost) OVER (PARTITION BY status), 4) AS cost_share
    FROM cleaned
    WINDOW w_grp AS (PARTITION BY order_id, status ORDER BY shipped_date_plus_24d ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        delivered_date,
        cost,
        shipped_date_plus_24d,
        last7_max_cost,
        last7_sum_cost,
        cost_share,
        AVG(last7_avg_cost) OVER (PARTITION BY status, order_id ORDER BY last7_avg_cost ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_last7_avg_cost,
        MAX(last7_avg_cost) OVER (PARTITION BY status, order_id ORDER BY last7_avg_cost ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_last7_avg_cost,
        MIN(last7_avg_cost) OVER (PARTITION BY status, order_id ORDER BY last7_avg_cost ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_last7_avg_cost
    FROM enriched
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY cost ASC NULLS LAST) AS dedup_rank
        FROM scored
    ) ranked
    WHERE dedup_rank = 1
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        delivered_date,
        cost,
        last7_max_cost,
        cost_share,
        last3_avg_last7_avg_cost,
        last3_max_last7_avg_cost,
        last3_min_last7_avg_cost,
        ROUND(last7_max_cost * 0.9, 2) AS last7_max_cost_adjusted,
        COALESCE(last7_max_cost, 0) AS last7_max_cost_filled
    FROM deduped
    WHERE delivered_date > DATE '2023-04-14'
),
scored_shipments AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(last7_max_cost_filled) OVER (PARTITION BY order_id ORDER BY last7_max_cost ASC, id ASC) AS prev_last7_max_cost_filled,
        LEAD(last3_avg_last7_avg_cost) OVER (PARTITION BY order_id ORDER BY last7_max_cost ASC, id ASC) AS next_last3_avg_last7_avg_cost,
        LAG(last3_avg_last7_avg_cost) OVER (PARTITION BY order_id ORDER BY last7_max_cost ASC, id ASC) AS prev_last3_avg_last7_avg_cost,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY last7_max_cost ASC, id ASC) AS event_seq,
        LEAD(last3_max_last7_avg_cost, 1) OVER (PARTITION BY order_id ORDER BY last7_max_cost_adjusted ASC, id ASC) AS next_last3_max_last7_avg_cost
    FROM valid
)
SELECT
    id,
    delivered_date,
    last7_max_cost,
    cost_share,
    last3_avg_last7_avg_cost,
    last3_min_last7_avg_cost,
    prev_last7_max_cost_filled,
    next_last3_avg_last7_avg_cost,
    prev_last3_avg_last7_avg_cost
FROM scored_shipments
ORDER BY last7_max_cost DESC
