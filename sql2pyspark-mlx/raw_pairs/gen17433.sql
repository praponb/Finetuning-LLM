WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        cost,
        status,
        ADD_MONTHS(shipped_date, 3) AS shipped_date_plus_months
    FROM shipments
    WHERE delivered_date > DATE '2023-07-14'
      AND YEAR(shipped_date) >= 2024
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN status = 'LOST' THEN 1 ELSE 0 END AS is_lost,
        COALESCE(carrier, 'UNKNOWN') AS carrier_filled
    FROM filtered
    WHERE carrier IS DISTINCT FROM 'FedEx'
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY is_lost DESC NULLS LAST, id) AS rank_in_group
    FROM cleaned
    QUALIFY rank_in_group = 1
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        carrier,
        cost,
        status,
        shipped_date_plus_months,
        rank_in_group,
        CEIL(is_lost) AS is_lost_whole,
        ROUND(DATEDIFF(shipped_date_plus_months, shipped_date) / 30, 1) AS months_shipped_date_to_shipped_date_plus_months,
        COALESCE(carrier, 'OTHER') AS carrier_filled_2
    FROM latest
    WHERE carrier IS NOT NULL
      AND shipped_date > DATE '2023-09-14'
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    carrier,
    metric,
    val
FROM (
    SELECT
        id,
        carrier,
        months_shipped_date_to_shipped_date_plus_months,
        cost
    FROM valid
) src
UNPIVOT (
    val FOR metric IN (months_shipped_date_to_shipped_date_plus_months, cost)
)
