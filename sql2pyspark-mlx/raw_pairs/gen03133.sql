WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        paid_at,
        status,
        CONCAT(method, ', ', status) method_status
    FROM payments
    WHERE paid_at >= TIMESTAMP '2023-11-14 14:00:00'
      AND method IS NOT DISTINCT FROM 'card'
)
-- Ranking and gap features
SELECT
    id,
    paid_at,
    status,
    method_status,
    ROW_NUMBER() OVER w_running seq_no,
    ROW_NUMBER() OVER w_running row_seq,
    ROW_NUMBER() OVER (PARTITION BY status ORDER BY paid_at DESC, id) seq_no_2,
    ROW_NUMBER() OVER w_running event_seq
FROM valid
WINDOW w_running AS (PARTITION BY status ORDER BY paid_at, id)
