WITH lines AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        pos + 1 AS item_no,
        item.product_id,
        item.price
    FROM carts
    LATERAL VIEW POSEXPLODE(items) t AS pos, item
),
metrics AS (
    SELECT
        id,
        item_no,
        LEAD(item_no) OVER w_ordered AS next_item_no,
        LAG(product_id) OVER w_ordered AS prev_product_id,
        LEAD(product_id) OVER w_ordered AS next_product_id,
        ROW_NUMBER() OVER w_ordered AS row_seq
    FROM lines
    WINDOW w_ordered AS (ORDER BY created_at, id, item_no)
),
metrics_carts AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY next_item_no DESC, id, item_no) AS seq_no,
        AVG(next_item_no) OVER (ORDER BY item_no, id, item_no ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS next_item_no_avg_3,
        ROW_NUMBER() OVER (ORDER BY prev_product_id DESC, id, item_no) AS row_seq_2,
        LAG(prev_product_id, 2) OVER (ORDER BY item_no, id, item_no) AS prev_prev_product_id,
        AVG(next_item_no) OVER (ORDER BY item_no, id, item_no ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS next_item_no_avg_6_prior
    FROM metrics
),
cleaned AS (
    SELECT *, next_item_no / NULLIF(item_no, 0) AS next_item_no_per_item_no
    FROM metrics_carts
    WHERE prev_product_id < 4
),
filtered AS (
    SELECT
        id,
        item_no,
        prev_product_id,
        next_product_id,
        row_seq,
        seq_no,
        next_item_no_avg_3,
        row_seq_2,
        next_item_no_avg_6_prior,
        next_item_no_per_item_no,
        COALESCE(next_item_no_avg_3, 0) AS next_item_no_avg_3_filled
    FROM cleaned
    WHERE next_item_no_avg_3 > 2
),
metrics_carts_2 AS (
    SELECT
        id,
        item_no,
        next_product_id,
        row_seq,
        seq_no,
        NTILE(3) OVER (ORDER BY next_item_no_avg_3_filled, id, item_no) AS next_item_no_avg_3_filled_bucket,
        RANK() OVER (ORDER BY row_seq NULLS FIRST) AS row_seq_rank
    FROM filtered
),
base AS (
    SELECT *, COALESCE(row_seq_rank, 0) AS row_seq_rank_filled
    FROM metrics_carts_2
    WHERE next_product_id = 9
)
SELECT
    id,
    item_no,
    next_product_id,
    next_item_no_avg_3_filled_bucket,
    row_seq_rank,
    row_seq_rank_filled
FROM base
ORDER BY next_product_id DESC
