WITH Base AS (
    SELECT *, CAST(is_flagged AS INT) AS is_flagged_flag
    FROM transactions
    WHERE is_flagged
),
summary AS (
    SELECT is_flagged, MAX(is_flagged_flag) - MIN(is_flagged_flag) AS is_flagged_flag_range, ROUND(AVG(amount), 2) AS avg_amount
    FROM Base
    GROUP BY is_flagged
)
SELECT *
FROM summary
