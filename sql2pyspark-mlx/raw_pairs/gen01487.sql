WITH joined AS (
    SELECT s.*, p.country
    FROM orders s
    JOIN customers p
        ON s.customer_id = p.id
),
with_last AS (
    SELECT
        s.id,
        s.customer_id,
        s.channel,
        s.discount,
        top1.payment_status
    FROM joined s
    JOIN LATERAL (
            SELECT c.status AS payment_status
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
enriched AS (
    SELECT
        id,
        customer_id,
        channel,
        discount,
        payment_status,
        LAG(discount) OVER (PARTITION BY channel, customer_id ORDER BY discount, id) AS prev_discount,
        ROW_NUMBER() OVER (PARTITION BY channel, customer_id ORDER BY discount, id) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY discount, id) AS event_seq_2
    FROM with_last
),
kept AS (
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.order_id = s.id
              AND x.quantity >= 6
        )
),
metrics AS (
    SELECT
        *,
        ROW_NUMBER() OVER w_ordered AS seq_no,
        FIRST_VALUE(channel) IGNORE NULLS OVER (PARTITION BY customer_id ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_channel,
        ROW_NUMBER() OVER w_ordered AS row_seq,
        CUME_DIST() OVER (PARTITION BY customer_id, payment_status ORDER BY prev_discount NULLS FIRST) AS prev_discount_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount, id) AS event_seq_3
    FROM kept
    WHERE discount IS NOT NULL
    WINDOW w_ordered AS (ORDER BY discount DESC, id)
),
scored AS (
    SELECT
        id,
        customer_id,
        payment_status,
        prev_discount,
        event_seq_2,
        first_known_channel,
        row_seq,
        prev_discount_cume_dist,
        MAX(prev_discount) OVER (PARTITION BY customer_id, channel ORDER BY seq_no, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_prev_discount,
        MIN(prev_discount) OVER (PARTITION BY customer_id, channel ORDER BY seq_no, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_prev_discount,
        SUM(prev_discount) OVER (PARTITION BY customer_id, channel ORDER BY seq_no, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_prev_discount,
        AVG(event_seq_3) OVER (PARTITION BY customer_id ORDER BY event_seq_3, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS event_seq_3_avg_5_prior
    FROM metrics
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY last4_sum_prev_discount DESC NULLS LAST, id) AS rn
        FROM scored
    ) ranked
    WHERE rn = 1
)
SELECT
    CASE WHEN last4_sum_prev_discount >= 2.19 THEN 'PREMIUM' ELSE 'STANDARD' END AS last4_sum_prev_discount_tier,
    customer_id,
    COUNT(*) FILTER (WHERE payment_status = 'SUCCESS') AS success_rows,
    ROUND(SUM(event_seq_2) / NULLIF(COUNT(CASE WHEN payment_status = 'SUCCESS' THEN 1 END), 0), 2) AS event_seq_2_per_success,
    PERCENTILE_APPROX(prev_discount, 0.9) AS prev_discount_p90,
    COUNT(*) AS event_count
FROM deduped
GROUP BY GROUPING SETS ((CASE WHEN last4_sum_prev_discount >= 2.19 THEN 'PREMIUM' ELSE 'STANDARD' END, customer_id), (CASE WHEN last4_sum_prev_discount >= 2.19 THEN 'PREMIUM' ELSE 'STANDARD' END))
