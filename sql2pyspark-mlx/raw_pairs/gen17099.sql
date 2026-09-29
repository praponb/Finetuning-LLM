WITH cleaned AS (
    SELECT
        id,
        account_type,
        DATEDIFF(CURRENT_DATE(), opened_date) / 7 AS weeks_since_opened_date,
        COALESCE(status, 'UNKNOWN') AS status_clean
    FROM accounts
    WHERE opened_date > DATE '2023-04-24'
)
SELECT s.*
FROM cleaned s
WHERE EXISTS (
        SELECT 1
        FROM transactions x
        WHERE x.account_id = s.id
          AND x.account_id IN (13, 19, 34)
    );
