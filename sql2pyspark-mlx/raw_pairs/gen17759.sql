WITH valid_customers AS (
    SELECT *
    FROM customers
    WHERE age <= 41
),
segment_summary AS (
    SELECT segment, QUARTER(signup_date) AS signup_quarter, ROUND((100 * SUM(CASE WHEN segment = 'SMB' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_smb
    FROM valid_customers
    GROUP BY segment, QUARTER(signup_date)
)
SELECT *
FROM segment_summary;
