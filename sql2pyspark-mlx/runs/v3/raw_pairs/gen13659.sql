WITH filtered_customers AS (
    SELECT *
    FROM customers
    WHERE age = 63 AND NOT is_active AND segment IN ('Corporate', 'SMB', 'Consumer')
),
SegmentSummary AS (
    SELECT segment, MAX(age) - MIN(age) AS age_range
    FROM filtered_customers
    GROUP BY segment
)
SELECT segment, age_range
FROM SegmentSummary
WHERE age_range < 31
