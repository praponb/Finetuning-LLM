WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        pos,
        li_item.product_id,
        li_item.qty,
        li_item.price,
        li_item.promo
    FROM carts
    LATERAL VIEW POSEXPLODE(items) e AS pos, li_item
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        created_at,
        coupon_codes,
        pos,
        product_id,
        qty,
        price,
        promo,
        NTILE(3) OVER (PARTITION BY promo ORDER BY price DESC, id ASC, pos ASC) AS price_bucket,
        LEAD(product_id, 1, 0) OVER w_recent AS next_product_id,
        ROW_NUMBER() OVER (PARTITION BY promo ORDER BY product_id DESC, id ASC, pos ASC) AS seq_no,
        NTILE(3) OVER (PARTITION BY promo ORDER BY qty DESC, id ASC, pos ASC) AS qty_bucket,
        LAG(pos, 2) OVER w_recent AS prev_pos
    FROM lines
    WINDOW w_recent AS (PARTITION BY promo ORDER BY qty ASC, id ASC, pos ASC)
),
enriched_carts AS (
    -- Ranking and gap features
    SELECT
        id,
        created_at,
        coupon_codes,
        pos,
        product_id,
        qty,
        price,
        promo,
        price_bucket,
        next_product_id,
        seq_no,
        qty_bucket,
        prev_pos,
        ROW_NUMBER() OVER w_ordered AS seq_no_2,
        SUM(prev_pos) OVER (PARTITION BY promo ORDER BY product_id ASC, id ASC, pos ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_prev_pos,
        RANK() OVER w_hist AS next_product_id_rank
    FROM enriched
    WINDOW w_ordered AS (ORDER BY pos DESC, id ASC, pos ASC), w_hist AS (PARTITION BY promo ORDER BY next_product_id DESC NULLS LAST)
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(qty, 2) OVER (PARTITION BY promo ORDER BY prev_pos ASC, id ASC, pos ASC) AS prev_qty,
        STDDEV_SAMP(seq_no_2) OVER w_recent AS promo_stddev_samp_seq_no_2
    FROM enriched_carts
    WINDOW w_recent AS (PARTITION BY promo)
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN created_at - LAG(created_at) OVER (PARTITION BY promo ORDER BY created_at ASC, id ASC, pos ASC) > INTERVAL '30' MINUTE THEN 1 ELSE 0 END AS is_new_session
    FROM scored
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY promo ORDER BY created_at ASC, id ASC, pos ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
summary AS (
    -- Grouped metrics
    SELECT
        seq_no,
        MAX(created_at) AS last_created_at,
        MAX(seq_no) FILTER (WHERE promo = 'SPRING10') AS spring10_max_seq_no,
        MAX(created_at) AS ended_at
    FROM islands
    GROUP BY seq_no
)
SELECT seq_no, spring10_max_seq_no
FROM summary
ORDER BY spring10_max_seq_no ASC, seq_no ASC
