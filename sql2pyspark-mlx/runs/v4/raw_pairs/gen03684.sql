WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        CASE WHEN carrier = 'Kerry' THEN 1 ELSE 0 END AS is_kerry,
        COALESCE(carrier, 'N/A') AS carrier_filled
    FROM shipments
    WHERE shipped_date > DATE '2024-03-05'
      AND carrier IS NOT NULL
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY order_id, carrier ORDER BY cost) AS cost_percent_rank,
        LAG(cost, 1, 0) OVER (PARTITION BY order_id ORDER BY delivered_date, id) AS prev_cost,
        ROUND(cost / SUM(cost) OVER (PARTITION BY carrier), 4) AS cost_share,
        LEAD(is_kerry, 2) OVER (PARTITION BY order_id ORDER BY shipped_date, id) AS next_is_kerry,
        SUM(is_kerry) OVER (PARTITION BY carrier ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_is_kerry
    FROM cleaned
),
cleaned_shipments AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(prev_cost, 0) AS prev_cost_filled,
        COALESCE(carrier, 'UNKNOWN') AS carrier_filled_2
    FROM enriched
    WHERE YEAR(delivered_date) >= 2023
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        is_kerry,
        carrier_filled,
        cost_percent_rank,
        prev_cost,
        cost_share,
        next_is_kerry,
        running_is_kerry,
        prev_cost_filled,
        carrier_filled_2,
        RANK() OVER (PARTITION BY order_id, carrier ORDER BY is_kerry DESC NULLS LAST) AS is_kerry_rank,
        ROUND(prev_cost / SUM(prev_cost) OVER (PARTITION BY order_id, carrier), 4) AS prev_cost_share
    FROM cleaned_shipments
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, carrier ORDER BY is_kerry DESC NULLS LAST, id) AS rank_in_group
    FROM scored
    QUALIFY rank_in_group = 1
)
-- Multi-level aggregation
SELECT
    CASE WHEN prev_cost_filled >= 82 THEN 'PREMIUM' ELSE 'STANDARD' END AS prev_cost_filled_tier,
    CASE WHEN prev_cost_share >= 0.64 THEN 'HIGH' ELSE 'LOW' END AS prev_cost_share_tier,
    COUNT(CASE WHEN carrier = 'Kerry' THEN 1 END) AS kerry_count,
    COUNT_IF(carrier = 'DHL') AS dhl_events,
    AVG(cost_share) AS avg_cost_share
FROM ranked
GROUP BY CASE WHEN prev_cost_filled >= 82 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN prev_cost_share >= 0.64 THEN 'HIGH' ELSE 'LOW' END
ORDER BY kerry_count;
