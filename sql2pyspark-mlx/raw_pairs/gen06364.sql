WITH valid_shipments AS (
    SELECT *, SUBSTRING(status, 1, 3) AS status_prefix
    FROM shipments
    WHERE (status IN ('LOST', 'IN_TRANSIT') AND delivered_date >= '2023-06-01') OR carrier NOT IN ('DHL', 'FedEx', 'Flash')
),
agg AS (
    SELECT CASE WHEN YEAR(shipped_date) < 2024 THEN 'before_2024' WHEN YEAR(shipped_date) = 2024 THEN '2024' ELSE 'after_2024' END AS shipped_cohort, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(status))) AS status_values, COUNT(CASE WHEN status = 'RETURNED' THEN 1 END) AS returned_count
    FROM valid_shipments
    GROUP BY CASE WHEN YEAR(shipped_date) < 2024 THEN 'before_2024' WHEN YEAR(shipped_date) = 2024 THEN '2024' ELSE 'after_2024' END
)
SELECT *
FROM agg
ORDER BY returned_count DESC;
