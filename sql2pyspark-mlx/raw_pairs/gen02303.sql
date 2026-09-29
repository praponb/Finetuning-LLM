WITH filtered AS (
    SELECT *, CASE WHEN is_flagged THEN 'Yes' ELSE 'No' END AS is_flagged_label
    FROM transactions
    WHERE merchant IS NOT NULL
),
joined AS (
    SELECT
        a.id,
        a.account_id,
        a.amount,
        a.txn_time,
        a.is_flagged_label,
        d.opened_date,
        d.status,
        d.balance
    FROM filtered a
    LEFT JOIN accounts d
        ON a.account_id = d.id
    WHERE d.account_type = 'LOAN'
),
enriched AS (
    SELECT
        s.id,
        s.account_id,
        s.is_flagged_label,
        s.opened_date,
        s.status,
        s.balance,
        a.account_type,
        a.status AS account_status,
        a.opened_date AS account_opened_date
    FROM joined s
    JOIN accounts a
        ON s.account_id = a.id
),
with_dims AS (
    SELECT
        e.*,
        b.balance AS account_balance,
        b.status AS account_status_2,
        b.opened_date AS account_opened_date_2
    FROM enriched e
    LEFT JOIN accounts b
        ON e.account_id = b.id
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY account_status_2, status ORDER BY opened_date DESC NULLS LAST, id) AS row_num
    FROM with_dims
    QUALIFY row_num = 1
)
SELECT *
FROM (
    SELECT
        account_status_2,
        account_status,
        account_balance
    FROM deduped
) src
PIVOT (
    MAX(account_balance) AS amount
    FOR account_status IN ('OPEN' AS open, 'CLOSED' AS closed)
);
