WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        ROUND(cost * 1.1, 2) AS cost_scaled,
        ROUND(cost, 0) AS cost_rounded,
        COALESCE(carrier, 'OTHER') AS resolved_carrier
    FROM shipments
    WHERE delivered_date >= TO_DATE('2024-01-08')
      AND carrier IS NOT NULL
),
filtered_shipments AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        carrier,
        shipped_date,
        delivered_date,
        status,
        cost_scaled,
        cost_rounded,
        CONCAT_WS('-', status, carrier) AS status_carrier_key
    FROM filtered
    WHERE delivered_date >= TO_DATE('2024-03-08')
      AND carrier IS NOT NULL
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(cost_scaled, 1) OVER (PARTITION BY status ORDER BY cost_scaled, id) AS next_cost_scaled,
        PERCENT_RANK() OVER (PARTITION BY carrier ORDER BY cost_scaled DESC) AS cost_scaled_percent_rank
    FROM filtered_shipments
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY cost_scaled DESC, id) AS seq_no,
        PERCENT_RANK() OVER (PARTITION BY status ORDER BY cost_rounded DESC NULLS FIRST) AS cost_rounded_percent_rank,
        AVG(next_cost_scaled) OVER (PARTITION BY status, carrier ORDER BY delivered_date, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS next_cost_scaled_avg_5
    FROM scored
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        carrier,
        shipped_date,
        delivered_date,
        status,
        cost_scaled,
        cost_rounded,
        status_carrier_key,
        next_cost_scaled,
        seq_no,
        cost_rounded_percent_rank,
        next_cost_scaled_avg_5,
        cost_scaled_percent_rank + next_cost_scaled_avg_5 AS cost_scaled_percent_rank_plus_next_cost_scaled_avg_5
    FROM with_windows
    WHERE cost_scaled <= 58
),
filtered_shipments_2 AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        carrier,
        shipped_date,
        delivered_date,
        status,
        cost_rounded,
        next_cost_scaled,
        cost_rounded_percent_rank,
        next_cost_scaled_avg_5,
        cost_scaled_percent_rank_plus_next_cost_scaled_avg_5,
        ROUND(cost_scaled * 1.1, 2) AS adjusted_cost_scaled,
        FLOOR(seq_no) AS seq_no_whole
    FROM base
    WHERE delivered_date >= DATE '2023-08-14'
      AND carrier IS NOT NULL
)
SELECT
    id,
    carrier,
    shipped_date,
    status,
    cost_rounded,
    next_cost_scaled,
    cost_scaled_percent_rank_plus_next_cost_scaled_avg_5,
    adjusted_cost_scaled
FROM filtered_shipments_2
ORDER BY next_cost_scaled DESC NULLS LAST, id DESC;
