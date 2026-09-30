WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER w_grp AS event_seq,
        ROW_NUMBER() OVER w_part AS event_seq_2,
        NTILE(3) OVER w_part_2 AS discount_bucket,
        CORR(discount, amount) OVER (PARTITION BY customer_id) AS corr_discount_amount,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY shipping_fee DESC, id) AS event_seq_3
    FROM orders
    WINDOW w_grp AS (ORDER BY shipping_fee DESC, id), w_part AS (PARTITION BY channel ORDER BY amount, id), w_part_2 AS (PARTITION BY customer_id ORDER BY discount DESC, id)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY status ORDER BY discount_bucket DESC NULLS LAST) AS discount_bucket_percent_rank,
        NTILE(4) OVER (PARTITION BY store_id ORDER BY shipping_fee, id) AS shipping_fee_bucket,
        AVG(amount) OVER (PARTITION BY status ORDER BY discount_bucket, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS amount_avg_3,
        SUM(event_seq_2) OVER (ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_event_seq_2
    FROM scored
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM with_windows src
    LEFT JOIN promotions pr
        ON src.channel <=> pr.channel
       AND src.order_date >= pr.starts_on
       AND src.order_date < pr.ends_on + INTERVAL '30' DAY
),
summary AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN discount_pct >= 0.3 THEN 'PREMIUM' ELSE 'STANDARD' END AS discount_pct_tier,
        DATE_TRUNC('MONTH', order_date) AS activity_month,
        MAX(event_seq_2) FILTER (WHERE status = 'SHIPPED') AS shipped_max_event_seq_2,
        COUNT(*) FILTER (WHERE status = 'SHIPPED') AS shipped_rows,
        COUNT(DISTINCT promo_code) AS distinct_promo_codes
    FROM with_window
    GROUP BY discount_pct_tier, activity_month
)
SELECT
    discount_pct_tier,
    activity_month,
    shipped_rows,
    distinct_promo_codes
FROM summary;
