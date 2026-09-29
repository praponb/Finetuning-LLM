WITH src AS (
    SELECT *
    FROM transactions
    WHERE txn_time >= '2023-11-01'
),
is_flagged_summary AS (
    SELECT
        is_flagged,
        txn_type,
        ROUND(AVG(amount), 2) AS avg_amount,
        SUM(CASE WHEN merchant = 'Lazada' THEN 1 ELSE 0 END) AS lazada_count
    FROM src
    GROUP BY is_flagged, txn_type
)
SELECT
    is_flagged,
    txn_type,
    avg_amount,
    lazada_count
FROM is_flagged_summary;
