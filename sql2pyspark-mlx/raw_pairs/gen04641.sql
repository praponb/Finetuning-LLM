WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        INITCAP(TRIM(status)) AS status_title,
        SUBSTRING(status, 1, 4) AS status_prefix
    FROM shipments
    WHERE order_id IS NOT NULL
),
remaining_keys AS (
    -- Set difference on keys
    SELECT order_id
    FROM base
    EXCEPT
    SELECT order_id
    FROM shipments
    WHERE cost > 89
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    JOIN remaining_keys k ON s.order_id = k.order_id
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status_title,
        status_prefix,
        SUM(cost) OVER (PARTITION BY carrier, order_id) AS carrier_sum_cost
    FROM verified
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status_title,
        carrier_sum_cost,
        LENGTH(carrier) AS carrier_length,
        COALESCE(carrier, 'N/A') AS carrier_filled
    FROM enriched
    WHERE delivered_date >= DATE '2023-10-01'
      AND carrier_sum_cost <= 478
),
base_shipments AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        carrier,
        delivered_date,
        carrier_sum_cost,
        carrier_length,
        carrier_filled,
        DATEDIFF(CURRENT_DATE(), delivered_date) AS days_since_delivered_date,
        COALESCE(carrier_length, 0) AS carrier_length_filled
    FROM cleaned
    WHERE carrier IS NOT NULL
      AND carrier_sum_cost >= 170
)
-- Multi-level aggregation
SELECT
    CASE WHEN carrier_sum_cost >= 450 THEN 'HIGH' ELSE 'LOW' END AS carrier_sum_cost_tier,
    CASE WHEN carrier_length >= 77 THEN 'PREMIUM' ELSE 'STANDARD' END AS carrier_length_tier,
    COUNT_IF(carrier = 'Flash') AS flash_events,
    SIZE(COLLECT_SET(carrier)) AS num_carriers,
    COUNT(CASE WHEN carrier = 'Flash' THEN 1 END) AS flash_count,
    MAX(carrier_length) FILTER (WHERE carrier = 'DHL') AS dhl_max_carrier_length
FROM base_shipments
GROUP BY carrier_sum_cost_tier, carrier_length_tier
HAVING COUNT(CASE WHEN carrier = 'Flash' THEN 1 END) > 1
ORDER BY carrier_sum_cost_tier NULLS LAST;
