WITH valid AS (
    SELECT
        *,
        TRIM(status) AS clean_status,
        COALESCE(channel, 'UNKNOWN') AS resolved_channel
    FROM orders
    WHERE channel IS NOT NULL
      AND order_date > TO_DATE('2023-08-09')
),
ranked AS (
    SELECT
        id,
        channel,
        amount,
        resolved_channel
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id, store_id ORDER BY discount DESC NULLS FIRST, id) = 1
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY amount DESC, id) AS row_num
        FROM ranked
    ) x
    WHERE row_num = 1
),
with_windows AS (
    SELECT
        *,
        LAG(amount) OVER (PARTITION BY channel ORDER BY amount, id) AS prev_amount,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY amount, id) AS event_seq
    FROM latest
)
SELECT
    id,
    channel,
    resolved_channel,
    row_num,
    prev_amount,
    event_seq
FROM with_windows
