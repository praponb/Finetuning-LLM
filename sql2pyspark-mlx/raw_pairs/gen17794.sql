WITH deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, method ORDER BY paid_at NULLS LAST, id) AS rank_in_group
        FROM payments
    ) x
    WHERE rank_in_group = 1
),
latest AS (
    SELECT
        id,
        method,
        amount,
        rank_in_group
    FROM deduped
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY paid_at DESC NULLS FIRST, id) = 1
),
scored AS (
    SELECT
        id,
        method,
        rank_in_group,
        NTILE(10) OVER (PARTITION BY method ORDER BY amount, id) AS amount_bucket,
        ROW_NUMBER() OVER w_hist AS row_seq
    FROM latest
    WHERE method = 'bank_transfer'
    WINDOW w_hist AS (PARTITION BY method ORDER BY amount DESC, id)
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY method ORDER BY rank_in_group, id) AS rn
    FROM scored
    QUALIFY rn = 1
),
scored_payments AS (
    SELECT
        *,
        LAG(row_seq) OVER (PARTITION BY method ORDER BY rn, id) AS prev_row_seq,
        LEAD(amount_bucket) OVER (PARTITION BY method ORDER BY rn, id) AS next_amount_bucket,
        ROW_NUMBER() OVER (PARTITION BY method ORDER BY rn, id) AS row_seq_2
    FROM ranked
)
SELECT
    method,
    APPROX_PERCENTILE(row_seq_2, ARRAY(0.25, 0.5, 0.75)) AS row_seq_2_quartiles,
    PERCENTILE_APPROX(row_seq_2, 0.95) AS row_seq_2_p95
FROM scored_payments
GROUP BY method
HAVING row_seq_2_p95 < 1000000;
