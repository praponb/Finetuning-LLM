WITH metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        order_id,
        product_id,
        LEAD(unit_price, 2) OVER (PARTITION BY product_id ORDER BY unit_price ASC, id ASC) AS next_unit_price,
        ROW_NUMBER() OVER (ORDER BY unit_price ASC, id ASC) AS event_seq,
        ROW_NUMBER() OVER (ORDER BY unit_price DESC, id ASC) AS seq_no
    FROM order_items
    WHERE product_id IS NOT NULL
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, event_seq - next_unit_price AS event_seq_minus_next_unit_price
    FROM metrics
    WHERE product_id IS NOT NULL
),
kept_keys AS (
    -- Set difference on keys
    SELECT order_id
    FROM cleaned
    EXCEPT DISTINCT
    SELECT order_id
    FROM order_items
    WHERE product_id IN (12, 21, 40)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    JOIN kept_keys k ON s.order_id = k.order_id
),
cleaned_order_items AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(event_seq_minus_next_unit_price * 1.1, 2) AS event_seq_minus_next_unit_price_adjusted
    FROM kept_rows
    WHERE next_unit_price > 240
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, seq_no - event_seq AS seq_no_minus_event_seq
    FROM cleaned_order_items
    WHERE product_id IS NOT NULL
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(next_unit_price * 1.15, 2) AS next_unit_price_scaled
    FROM filtered
    WHERE event_seq_minus_next_unit_price_adjusted BETWEEN 42 AND 72
),
scored AS (
    -- Running and moving aggregates
    SELECT *, COUNT(seq_no_minus_event_seq) OVER (PARTITION BY product_id, order_id) AS product_id_count_seq_no_minus_event_seq
    FROM valid
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN next_unit_price_scaled >= 81 THEN 'HIGH' ELSE 'LOW' END AS next_unit_price_scaled_tier,
    seq_no,
    COUNT(*) AS cnt,
    COUNT(*) AS num_rows,
    PERCENTILE_APPROX(seq_no_minus_event_seq, 0.5) AS seq_no_minus_event_seq_p50,
    GROUPING(CASE WHEN next_unit_price_scaled >= 81 THEN 'HIGH' ELSE 'LOW' END) AS gid
FROM scored
GROUP BY GROUPING SETS ((CASE WHEN next_unit_price_scaled >= 81 THEN 'HIGH' ELSE 'LOW' END, seq_no), (CASE WHEN next_unit_price_scaled >= 81 THEN 'HIGH' ELSE 'LOW' END), ())
HAVING PERCENTILE_APPROX(seq_no_minus_event_seq, 0.5) > 0
