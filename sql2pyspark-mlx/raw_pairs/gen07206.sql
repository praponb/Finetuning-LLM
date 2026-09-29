WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        carrier,
        shipped_date,
        DATEDIFF(delivered_date, shipped_date) / 7 AS weeks_shipped_date_to_delivered_date,
        COALESCE(carrier, 'NONE') AS carrier_filled
    FROM shipments
    WHERE shipped_date > DATE '2023-03-29'
      AND carrier IS NOT NULL
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        carrier,
        shipped_date,
        weeks_shipped_date_to_delivered_date,
        carrier_filled
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY shipped_date DESC, id) AS row_num
        FROM filtered
    ) t
    WHERE row_num = 1
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY weeks_shipped_date_to_delivered_date NULLS LAST, id) AS dedup_rank
        FROM deduped
    ) t
    WHERE dedup_rank = 1
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT carrier, id
    FROM ranked
    INTERSECT ALL
    SELECT carrier, id
    FROM shipments
    WHERE order_id = 26
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM ranked s
    INNER JOIN remaining_keys k ON s.carrier = k.carrier AND s.id = k.id
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        shipped_date,
        carrier_filled,
        dedup_rank,
        ROUND(weeks_shipped_date_to_delivered_date * 100, 2) AS weeks_shipped_date_to_delivered_date_adjusted
    FROM reconciled
    WHERE shipped_date >= DATE '2023-03-02'
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN weeks_shipped_date_to_delivered_date_adjusted >= 87 THEN 'LARGE' ELSE 'SMALL' END AS weeks_shipped_date_to_delivered_date_adjusted_tier,
    DATE_TRUNC('QUARTER', shipped_date) AS quarter_start,
    ROUND(AVG(weeks_shipped_date_to_delivered_date_adjusted), 2) AS avg_weeks_shipped_date_to_delivered_date_adjusted,
    ROUND(AVG(weeks_shipped_date_to_delivered_date_adjusted), 2) AS avg_weeks_shipped_date_to_delivered_date_adjusted_2,
    APPROX_PERCENTILE(weeks_shipped_date_to_delivered_date_adjusted, 0.95) AS weeks_shipped_date_to_delivered_date_adjusted_p95,
    PERCENTILE_APPROX(weeks_shipped_date_to_delivered_date_adjusted, ARRAY(0.25, 0.5, 0.75)) AS weeks_shipped_date_to_delivered_date_adjusted_quartiles
FROM cleaned
GROUP BY CASE WHEN weeks_shipped_date_to_delivered_date_adjusted >= 87 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('QUARTER', shipped_date)
HAVING APPROX_PERCENTILE(weeks_shipped_date_to_delivered_date_adjusted, 0.95) < 1000000
ORDER BY avg_weeks_shipped_date_to_delivered_date_adjusted DESC NULLS FIRST
