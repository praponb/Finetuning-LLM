WITH deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, method ORDER BY paid_at, id) AS dedup_rank
    FROM payments
    QUALIFY dedup_rank = 1
)
SELECT
    order_id,
    MAX(amount) AS max_amount,
    SUM(amount) FILTER (WHERE status = 'SUCCESS') AS success_total_amount,
    ROUND(SUM(amount) / NULLIF(COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END), 0), 2) AS amount_per_success,
    ROUND(SUM(amount) / NULLIF(COUNT(CASE WHEN method = 'bank_transfer' THEN 1 END), 0), 2) AS amount_per_bank_transfer
FROM deduped
GROUP BY order_id
