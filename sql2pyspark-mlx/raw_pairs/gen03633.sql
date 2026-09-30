WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, TRIM(status) AS clean_status
    FROM payments
    WHERE paid_at > TO_DATE('2023-11-12')
      AND method IS NOT NULL
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        clean_status,
        LEAD(amount) OVER w_grp AS next_amount,
        ROW_NUMBER() OVER w_grp AS event_seq,
        SUM(amount) OVER w_grp_2 AS cumulative_amount
    FROM valid
    WINDOW w_grp AS (PARTITION BY order_id ORDER BY amount, id), w_grp_2 AS (PARTITION BY DATE_TRUNC('WEEK', paid_at), order_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows_payments AS (
    -- Running and moving aggregates
    SELECT
        id,
        clean_status,
        event_seq,
        cumulative_amount,
        LEAD(event_seq) OVER (ORDER BY event_seq, id) AS next_event_seq,
        LAG(next_amount) OVER (ORDER BY event_seq, id) AS prev_next_amount,
        LEAD(next_amount) OVER (ORDER BY event_seq, id) AS next_next_amount,
        ROW_NUMBER() OVER (ORDER BY event_seq, id) AS row_seq,
        LAST_VALUE(next_amount, TRUE) OVER (ORDER BY cumulative_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_next_amount
    FROM with_windows
    WHERE event_seq < 4
),
valid_payments AS (
    -- Keep recent, valid rows
    SELECT
        id,
        clean_status,
        event_seq,
        cumulative_amount,
        next_event_seq,
        prev_next_amount,
        next_next_amount,
        row_seq,
        last_known_next_amount,
        event_seq + row_seq AS event_seq_plus_row_seq
    FROM with_windows_payments
    WHERE cumulative_amount < 4223
),
valid_payments_2 AS (
    -- Keep recent, valid rows
    SELECT
        id,
        next_event_seq,
        prev_next_amount,
        next_next_amount,
        row_seq,
        FLOOR(prev_next_amount) AS prev_next_amount_whole,
        GREATEST(cumulative_amount, last_known_next_amount) AS greatest_cumulative_amount_last_known_next_amount
    FROM valid_payments
    WHERE cumulative_amount <> 5486
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN greatest_cumulative_amount_last_known_next_amount >= 73 THEN 'HIGH' ELSE 'LOW' END AS greatest_cumulative_amount_last_known_next_amount_tier,
    CASE WHEN row_seq >= 9 THEN 'HIGH' ELSE 'LOW' END AS row_seq_tier,
    CASE WHEN next_next_amount >= 1590 THEN 'HIGH' ELSE 'LOW' END AS next_next_amount_tier,
    MIN(prev_next_amount_whole) AS min_prev_next_amount_whole,
    APPROX_PERCENTILE(greatest_cumulative_amount_last_known_next_amount, 0.9) AS greatest_cumulative_amount_last_known_next_amount_p90,
    COUNT(*) AS num_rows,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) AS num_rows_2,
    PERCENTILE_APPROX(prev_next_amount_whole, ARRAY(0.25, 0.5, 0.75)) AS prev_next_amount_whole_quartiles
FROM valid_payments_2
GROUP BY greatest_cumulative_amount_last_known_next_amount_tier, row_seq_tier, next_next_amount_tier
ORDER BY num_rows DESC;
