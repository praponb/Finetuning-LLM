WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        INITCAP(TRIM(status)) AS status_title,
        CONCAT_WS(',', carrier, status) AS carrier_status_key
    FROM shipments
    WHERE delivered_date >= DATE '2023-03-12'
      AND status = 'IN_TRANSIT'
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATEDIFF(delivered_date, shipped_date) / 7 AS weeks_shipped_date_to_delivered_date,
        COALESCE(carrier, 'N/A') AS carrier_clean
    FROM cleaned
    WHERE shipped_date >= TO_DATE('2023-06-28')
      AND order_id IS NOT NULL
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        status,
        weeks_shipped_date_to_delivered_date,
        carrier_clean
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY delivered_date DESC NULLS FIRST, id) = 1
),
joined AS (
    -- Attach orders attributes
    SELECT
        s.id,
        s.order_id,
        s.delivered_date,
        s.status,
        s.weeks_shipped_date_to_delivered_date,
        p.order_date
    FROM ranked s
    JOIN orders p
        ON s.order_id = p.id
),
pivoted AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            order_id,
            status,
            weeks_shipped_date_to_delivered_date
        FROM joined
    ) src
    PIVOT (
        MAX(weeks_shipped_date_to_delivered_date) AS total
        FOR status IN ('DELIVERED' AS delivered, 'RETURNED' AS returned, 'IN_TRANSIT' AS in_transit)
    )
)
SELECT
    order_id,
    delivered,
    returned
FROM pivoted
ORDER BY returned NULLS FIRST;
