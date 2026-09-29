WITH base AS (
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        merchant,
        is_flagged,
        COALESCE(amount, 0) AS amount_filled,
        CASE WHEN is_flagged THEN 'Yes' ELSE 'No' END AS is_flagged_label,
        COALESCE(merchant, 'N/A') AS resolved_merchant
    FROM transactions
    WHERE merchant IS NOT NULL
      AND txn_time >= '2024-02-25'
),
enriched AS (
    SELECT
        s.*,
        p.account_type,
        p.status,
        p.opened_date
    FROM base s
    LEFT JOIN accounts p
        ON s.account_id = p.id
),
scored AS (
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY account_type ORDER BY amount DESC, id) AS amount_bucket,
        ROW_NUMBER() OVER (PARTITION BY account_id, account_type ORDER BY amount_filled DESC, id) AS row_seq,
        AVG(amount) OVER (PARTITION BY account_id) AS account_id_avg_amount,
        AVG(amount) OVER (PARTITION BY account_id, txn_type ORDER BY amount, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS amount_avg_3
    FROM enriched
)
SELECT *
FROM (
    SELECT
        merchant,
        txn_type,
        row_seq
    FROM scored
) src
PIVOT (
    SUM(row_seq) AS total
    FOR txn_type IN ('DEBIT' AS debit, 'CREDIT' AS credit)
);
