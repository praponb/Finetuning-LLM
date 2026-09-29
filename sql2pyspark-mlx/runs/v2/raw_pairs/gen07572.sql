WITH filtered_shipments AS (
    SELECT *, YEAR(delivered_date) AS delivered_date_year
    FROM shipments
    WHERE cost IS NULL AND cost IS NULL
),
summary AS (
    SELECT carrier, MAX(cost) - MIN(cost) AS cost_range, MAX(shipped_date) AS last_shipped_date
    FROM filtered_shipments
    GROUP BY carrier
)
SELECT *
FROM summary;
