WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT_WS('-', carrier, status) AS carrier_status_key,
        LOWER(carrier) AS carrier_lower,
        COALESCE(carrier, 'NONE') AS carrier_filled
    FROM shipments
    WHERE delivered_date >= TO_DATE('2023-04-13')
      AND carrier IS NOT NULL
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY status, carrier ORDER BY delivered_date DESC NULLS LAST) AS row_num
        FROM filtered
    ) t
    WHERE row_num = 1
),
filtered_shipments AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(row_num * 0.85, 2) AS row_num_adjusted
    FROM latest
    WHERE delivered_date >= DATE '2023-09-17'
      AND order_id IS NOT NULL
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        carrier,
        cost,
        carrier_status_key,
        carrier_lower,
        carrier_filled,
        PERCENT_RANK() OVER (PARTITION BY order_id ORDER BY row_num_adjusted ASC) AS row_num_adjusted_percent_rank,
        NTILE(5) OVER (ORDER BY row_num_adjusted DESC, id ASC) AS row_num_adjusted_bucket
    FROM filtered_shipments
    WHERE cost < 96
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LOWER(carrier) AS carrier_lower_2,
        CAST(cost AS INT) AS cost_int
    FROM enriched
    WHERE carrier IS NOT NULL
),
wide AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            order_id,
            carrier,
            row_num_adjusted_bucket
        FROM valid
    ) src
    PIVOT (
        SUM(row_num_adjusted_bucket) AS amount,
        COUNT(1) AS item_count
        FOR carrier IN ('UPS' AS ups, 'Flash' AS flash)
    )
)
SELECT
    order_id,
    ups_amount,
    ups_item_count,
    flash_amount,
    flash_item_count
FROM wide
