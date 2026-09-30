WITH cleaned AS (
    SELECT *, CASE WHEN channel = 'store' THEN 1 ELSE 0 END is_store
    FROM orders
    WHERE channel IS DISTINCT FROM 'store'
      AND store_id IS NOT NULL
),
attributed AS (
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM cleaned e
    LEFT JOIN promotions pr
        ON e.channel <=> pr.channel
       AND e.order_date >= pr.starts_on
       AND e.order_date < pr.ends_on + INTERVAL 7 DAYS
),
ranked AS (
    SELECT
        id,
        discount,
        promo_id,
        promo_code,
        discount_pct
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY amount, id, promo_id) AS row_num
        FROM attributed
    ) t
    WHERE row_num = 1
),
ranked_orders AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY discount DESC, id, promo_id) AS rn
    FROM ranked
    QUALIFY rn = 1
),
with_windows AS (
    SELECT
        id,
        promo_id,
        discount_pct,
        LAG(discount_pct) OVER (PARTITION BY promo_code ORDER BY rn, id, promo_id) prev_discount_pct,
        LEAD(discount_pct) OVER (PARTITION BY promo_code ORDER BY rn, id, promo_id) next_discount_pct,
        LEAD(discount) OVER (PARTITION BY promo_code ORDER BY rn, id, promo_id) next_discount,
        LAG(discount) OVER (PARTITION BY promo_code ORDER BY rn, id, promo_id) prev_discount,
        ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY rn, id, promo_id) event_seq
    FROM ranked_orders
),
metrics AS (
    SELECT
        id,
        promo_id,
        discount_pct,
        next_discount_pct,
        next_discount,
        event_seq,
        ROW_NUMBER() OVER (ORDER BY discount_pct, id, promo_id) event_seq_2,
        ROW_NUMBER() OVER (ORDER BY prev_discount, id, promo_id) seq_no,
        PERCENT_RANK() OVER (ORDER BY event_seq DESC NULLS FIRST) event_seq_percent_rank,
        LAST_VALUE(prev_discount, TRUE) OVER (ORDER BY prev_discount_pct, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_prev_discount
    FROM with_windows
),
remaining_keys AS (
    SELECT id
    FROM metrics
    EXCEPT DISTINCT
    SELECT id
    FROM orders
    WHERE store_id IS NULL
),
kept_rows AS (
    SELECT s.*
    FROM metrics s
    INNER JOIN remaining_keys k ON s.id = k.id
)
SELECT
    CASE WHEN next_discount >= 0.39 THEN 'PREMIUM' ELSE 'STANDARD' END AS next_discount_tier,
    seq_no,
    CASE WHEN discount_pct >= 0.36 THEN 'HIGH' ELSE 'LOW' END AS discount_pct_tier,
    APPROX_PERCENTILE(promo_id, 0.5) AS promo_id_p50,
    ROUND(AVG(promo_id), 2) AS avg_promo_id,
    PERCENTILE_APPROX(promo_id, 0.95) AS promo_id_p95,
    GROUPING(CASE WHEN next_discount >= 0.39 THEN 'PREMIUM' ELSE 'STANDARD' END) AS gid
FROM kept_rows
GROUP BY CUBE (CASE WHEN next_discount >= 0.39 THEN 'PREMIUM' ELSE 'STANDARD' END, seq_no, CASE WHEN discount_pct >= 0.36 THEN 'HIGH' ELSE 'LOW' END)
HAVING ROUND(AVG(promo_id), 2) > 0;
