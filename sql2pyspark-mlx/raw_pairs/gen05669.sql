WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        DATEDIFF(shipped_date, delivered_date) / 7 AS weeks_delivered_date_to_shipped_date,
        COALESCE(carrier, 'N/A') AS carrier_filled
    FROM shipments
    WHERE delivered_date >= DATE '2023-07-24'
      AND order_id IS NOT NULL
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        carrier,
        delivered_date
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY shipped_date DESC NULLS LAST, id ASC) = 1
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LOWER(carrier) AS carrier_lower,
        TRIM(carrier) AS clean_carrier
    FROM ranked
    WHERE carrier IS NOT NULL
      AND delivered_date < '2025-08-02'
),
ranked_shipments AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY carrier ORDER BY delivered_date DESC) AS rn
        FROM filtered
    ) t
    WHERE rn = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY delivered_date DESC, id ASC) AS seq_no,
        LAST_VALUE(carrier, TRUE) OVER (PARTITION BY carrier ORDER BY delivered_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_carrier,
        ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY rn ASC, id ASC) AS seq_no_2
    FROM ranked_shipments
    WHERE delivered_date BETWEEN '2023-10-08' AND '2024-08-01'
),
report AS (
    SELECT
        id,
        carrier,
        carrier_lower,
        clean_carrier,
        rn,
        seq_no,
        last_known_carrier,
        seq_no_2
    FROM with_windows
)
SELECT
    id,
    carrier_lower,
    clean_carrier,
    rn,
    seq_no,
    last_known_carrier,
    seq_no_2
FROM report
ORDER BY seq_no DESC NULLS LAST, id ASC;
