WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CAST(unit_price AS INT) AS unit_price_int,
        quantity + unit_price AS line_total
    FROM order_items
    WHERE product_id IS NOT NULL
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY line_total DESC, id) AS row_num
    FROM cleaned
    QUALIFY row_num = 1
),
ranked_order_items AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity, id) AS dedup_rank
        FROM ranked
    ) x
    WHERE dedup_rank = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(line_total) OVER (PARTITION BY order_id ORDER BY row_num, id) AS prev_line_total,
        LEAD(line_total) OVER (PARTITION BY order_id ORDER BY row_num, id) AS next_line_total,
        LEAD(quantity) OVER (PARTITION BY order_id ORDER BY row_num, id) AS next_quantity,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY row_num, id) AS event_seq,
        AVG(unit_price_int) OVER (PARTITION BY product_id, order_id ORDER BY unit_price_int, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS unit_price_int_avg_3
    FROM ranked_order_items
),
scored_order_items AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(3) OVER w_hist AS unit_price_int_bucket,
        NTILE(5) OVER (ORDER BY line_total, id) AS line_total_bucket,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price_int DESC, id) AS row_seq,
        ROW_NUMBER() OVER (ORDER BY unit_price_int DESC, id) AS seq_no,
        LAG(next_line_total, 2) OVER (PARTITION BY order_id ORDER BY dedup_rank, id) AS prev_next_line_total
    FROM scored
    WHERE next_quantity = 7
    WINDOW w_hist AS (PARTITION BY product_id ORDER BY unit_price_int, id)
),
enriched AS (
    -- Attach products attributes
    SELECT
        e.id,
        e.order_id,
        e.product_id,
        e.quantity,
        e.unit_price_int,
        e.prev_line_total,
        e.next_line_total,
        e.event_seq,
        e.unit_price_int_bucket,
        e.line_total_bucket,
        e.row_seq,
        e.seq_no,
        e.prev_next_line_total,
        p.price
    FROM scored_order_items e
    LEFT JOIN products p
        ON e.product_id = p.id
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price_int,
        prev_line_total,
        line_total_bucket,
        row_seq,
        seq_no,
        prev_next_line_total,
        price,
        CAST(prev_line_total AS INT) AS prev_line_total_int,
        COALESCE(prev_next_line_total, 0) AS prev_next_line_total_filled
    FROM enriched
    WHERE quantity < 8
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN prev_line_total_int >= 71 THEN 'HIGH' ELSE 'LOW' END AS prev_line_total_int_tier,
        seq_no,
        order_id,
        MIN(unit_price_int) AS min_unit_price_int,
        PERCENTILE_APPROX(prev_next_line_total_filled, ARRAY(0.25, 0.5, 0.75)) AS prev_next_line_total_filled_quartiles,
        ROUND(AVG(row_seq), 2) AS avg_row_seq,
        BIT_AND(quantity) AS quantity_bit_and,
        BIT_AND(quantity) AS quantity_bit_and_2
    FROM base
    GROUP BY CASE WHEN prev_line_total_int >= 71 THEN 'HIGH' ELSE 'LOW' END, seq_no, order_id
    HAVING avg_row_seq < 1000000
)
SELECT
    s.prev_line_total_int_tier,
    s.seq_no,
    s.order_id,
    s.min_unit_price_int,
    s.quantity_bit_and,
    s.quantity_bit_and_2,
    (SELECT MAX(x.amount) FROM payments x WHERE x.order_id = s.order_id) AS max_payment_amount
FROM summary s;
