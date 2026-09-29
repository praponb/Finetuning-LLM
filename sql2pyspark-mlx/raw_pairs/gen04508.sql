WITH valid AS (
    SELECT
        id,
        customer_id,
        device,
        TO_DATE(started_at) AS started_at_date
    FROM sessions
    WHERE browser NOT IN ('Chrome', 'Safari')
)
SELECT
    id,
    customer_id,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at_date ASC, id ASC) AS event_seq,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at_date DESC, id ASC) AS event_seq_2,
    ROW_NUMBER() OVER w_part AS seq_no,
    ROW_NUMBER() OVER w_grp AS event_seq_3
FROM valid
WINDOW w_part AS (PARTITION BY device, customer_id ORDER BY started_at_date ASC, id ASC), w_grp AS (PARTITION BY device ORDER BY started_at_date ASC, id ASC)
