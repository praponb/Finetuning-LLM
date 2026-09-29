WITH exploded AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        k,
        attr_value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) li AS k, attr_value
),
cleaned AS (
    SELECT *, COALESCE(k, 'unassigned') AS k_filled
    FROM exploded
    WHERE created_at > TO_DATE('2025-03-01')
      AND customer_id IS NOT NULL
),
enriched AS (
    SELECT
        id,
        customer_id,
        coupon_codes,
        k,
        attr_value,
        k_filled,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at DESC, id, k) AS row_seq,
        FIRST_VALUE(k) IGNORE NULLS OVER (ORDER BY created_at, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_k,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at DESC, id, k) AS row_seq_2,
        ROW_NUMBER() OVER (ORDER BY created_at, id, k) AS event_seq
    FROM cleaned
),
enriched_carts AS (
    SELECT
        id,
        customer_id,
        k,
        attr_value,
        row_seq,
        MAX(event_seq) OVER (PARTITION BY k ORDER BY row_seq, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_event_seq,
        MIN(event_seq) OVER (PARTITION BY k ORDER BY row_seq, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_event_seq,
        COUNT(event_seq) OVER (PARTITION BY k ORDER BY row_seq, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_event_seq,
        LAST_VALUE(event_seq) IGNORE NULLS OVER (ORDER BY row_seq, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_seq
    FROM enriched
)
SELECT
    s.id,
    s.customer_id,
    s.k,
    s.attr_value,
    s.running_min_event_seq,
    s.running_count_event_seq,
    s.last_known_event_seq,
    CASE WHEN k RLIKE '^(utm_source|device)$' THEN 'EXTENDED' ELSE 'PRIORITY' END AS k_zone,
    (SELECT AVG(x.discount) FROM orders x WHERE x.customer_id = s.customer_id) AS avg_order_discount
FROM enriched_carts s
ORDER BY running_min_event_seq DESC NULLS FIRST;
