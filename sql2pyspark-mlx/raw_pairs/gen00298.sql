WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LAST_DAY(delivered_date) AS delivered_date_month_end,
        COALESCE(carrier, 'N/A') AS carrier_filled
    FROM shipments
    WHERE shipped_date >= DATE '2023-03-11'
      AND carrier IS DISTINCT FROM 'FedEx'
),
cleaned_shipments AS (
    -- Keep recent, valid rows
    SELECT
        id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        carrier_filled,
        DATEDIFF(shipped_date, delivered_date) AS days_delivered_date_to_shipped_date,
        DATEDIFF(delivered_date_month_end, shipped_date) AS days_shipped_date_to_delivered_date_month_end,
        COALESCE(carrier, 'N/A') AS carrier_clean
    FROM cleaned
    WHERE carrier IS NOT NULL
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status, carrier ORDER BY days_delivered_date_to_shipped_date DESC NULLS FIRST, id) AS dedup_rank
    FROM cleaned_shipments
    QUALIFY dedup_rank = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        days_shipped_date_to_delivered_date_month_end,
        LEAD(cost) OVER (PARTITION BY carrier ORDER BY shipped_date, id) AS next_cost,
        LAG(days_delivered_date_to_shipped_date) OVER (PARTITION BY carrier ORDER BY shipped_date, id) AS prev_days_delivered_date_to_shipped_date,
        ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY shipped_date, id) AS seq_no
    FROM deduped
    WHERE carrier <> 'DHL'
)
-- Multi-level aggregation
SELECT
    CASE WHEN seq_no >= 7 THEN 'HIGH' ELSE 'LOW' END AS seq_no_tier,
    carrier,
    COUNT(*) AS row_count,
    SIZE(COLLECT_SET(status)) AS num_status,
    SUM(CASE WHEN carrier = 'Flash' THEN cost ELSE 0 END) AS flash_cost,
    COUNT(DISTINCT carrier) AS distinct_carriers,
    GROUPING(CASE WHEN seq_no >= 7 THEN 'HIGH' ELSE 'LOW' END) AS gid
FROM enriched
GROUP BY ROLLUP (CASE WHEN seq_no >= 7 THEN 'HIGH' ELSE 'LOW' END, carrier)
HAVING distinct_carriers > 1;
