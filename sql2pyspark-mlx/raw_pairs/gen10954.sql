WITH filtered AS (
    SELECT
        id,
        order_id,
        method,
        paid_at,
        status,
        CAST(amount AS INT) amount_int,
        MONTH(paid_at) paid_at_month
    FROM payments
    WHERE paid_at > TIMESTAMP '2023-09-18 11:00:00'
      AND order_id IS NOT NULL
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status, order_id ORDER BY paid_at_month DESC NULLS LAST, id) AS rank_in_group
        FROM filtered
    ) ranked
    WHERE rank_in_group = 1
),
latest AS (
    SELECT
        id,
        order_id,
        rank_in_group
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status, order_id ORDER BY rank_in_group DESC, id) AS dedup_rank
        FROM ranked
    ) t
    WHERE dedup_rank = 1
),
latest_payments AS (
    SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY rank_in_group DESC) AS row_num
    FROM latest
    QUALIFY row_num = 1
)
SELECT
    s.id,
    s.order_id,
    s.rank_in_group,
    (SELECT MIN(x.amount) FROM payments x WHERE x.order_id = s.order_id) AS min_payment_amount
FROM latest_payments s
ORDER BY rank_in_group DESC NULLS FIRST;
