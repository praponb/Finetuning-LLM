WITH filtered AS (
    SELECT
        id,
        shipped_date,
        delivered_date,
        status,
        TRIM(status) clean_status,
        LAST_DAY(delivered_date) delivered_date_month_end
    FROM shipments
    WHERE order_id IS NOT NULL
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY delivered_date, id) AS rn
        FROM filtered
    ) t
    WHERE rn = 1
),
remaining_keys AS (
    SELECT status, id
    FROM deduped
    INTERSECT ALL
    SELECT status, id
    FROM shipments
    WHERE delivered_date BETWEEN '2024-04-01' AND '2025-10-19'
),
kept_rows AS (
    SELECT s.*
    FROM deduped s
    INNER JOIN remaining_keys k ON s.status = k.status AND s.id = k.id
),
valid AS (
    SELECT
        id,
        shipped_date,
        delivered_date,
        status,
        clean_status,
        rn,
        DATE_ADD(shipped_date, 16) shipped_date_plus_16d
    FROM kept_rows
    WHERE delivered_date > DATE '2023-04-07'
      AND MONTH(shipped_date) IN (5, 6, 11)
),
valid_shipments AS (
    SELECT
        id,
        shipped_date,
        delivered_date,
        status,
        clean_status,
        rn,
        shipped_date_plus_16d,
        DATEDIFF(CURRENT_DATE(), shipped_date) days_since_shipped_date
    FROM valid
    WHERE delivered_date BETWEEN '2023-11-01' AND '2025-10-01'
)
SELECT
    id,
    shipped_date,
    delivered_date,
    status,
    clean_status,
    rn,
    shipped_date_plus_16d,
    days_since_shipped_date,
    RANK() OVER (PARTITION BY status ORDER BY days_since_shipped_date DESC NULLS FIRST) days_since_shipped_date_rank,
    MAX(days_since_shipped_date) OVER (PARTITION BY status) status_max_days_since_shipped_date,
    PERCENT_RANK() OVER (ORDER BY days_since_shipped_date NULLS LAST) days_since_shipped_date_percent_rank,
    SUM(days_since_shipped_date) OVER (PARTITION BY status) status_sum_days_since_shipped_date
FROM valid_shipments
