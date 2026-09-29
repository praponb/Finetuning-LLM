WITH base AS (
    SELECT *
    FROM accounts
    WHERE status NOT IN ('FROZEN', 'OPEN', 'CLOSED') AND balance <= 69000
),
balance_bucket_summary AS (
    SELECT CASE WHEN balance >= 81000 THEN 'high' WHEN balance >= 14000 THEN 'mid' ELSE 'low' END AS balance_bucket, SUM(CASE WHEN status = 'CLOSED' THEN 1 ELSE 0 END) AS closed_count
    FROM base
    GROUP BY CASE WHEN balance >= 81000 THEN 'high' WHEN balance >= 14000 THEN 'mid' ELSE 'low' END
)
SELECT balance_bucket, closed_count
FROM balance_bucket_summary;
