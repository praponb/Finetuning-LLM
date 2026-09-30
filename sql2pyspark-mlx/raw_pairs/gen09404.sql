WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, IF(is_active, 'Yes', 'No') AS is_active_label
    FROM customers
    WHERE LENGTH(email) > 6
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        city,
        segment,
        credit_limit
    FROM valid
) src
PIVOT (
    SUM(credit_limit) AS total
    FOR segment IN ('Corporate' AS corporate, 'SMB' AS smb, 'Consumer' AS consumer)
);
