WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_name,
        opened_date,
        sqft,
        LAG(sqft, 1, 0) OVER w_hist AS prev_sqft,
        ROW_NUMBER() OVER w_running AS row_seq,
        LEAD(sqft, 1) OVER (ORDER BY sqft, id) AS next_sqft,
        NTILE(10) OVER (PARTITION BY region ORDER BY sqft DESC, id) AS sqft_bucket
    FROM stores
    WINDOW w_hist AS (ORDER BY opened_date, id), w_running AS (PARTITION BY city ORDER BY opened_date, id)
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.store_id = 5
        )
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY opened_date, id) AS seq_no,
        NTILE(5) OVER (ORDER BY sqft DESC, id) AS sqft_bucket_2,
        ROW_NUMBER() OVER (ORDER BY sqft DESC, id) AS event_seq
    FROM kept
),
kept_stores AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE EXISTS (
            SELECT 1
            FROM orders x
            WHERE x.store_id = s.id
              AND x.amount BETWEEN 1180 AND 1260
        )
)
-- Aggregate with conditional metrics
SELECT
    seq_no,
    DATE_TRUNC('MONTH', opened_date) AS activity_month,
    BIT_AND(sqft) AS sqft_bit_and,
    ROUND(AVG(sqft_bucket_2), 2) AS avg_sqft_bucket_2,
    BIT_OR(sqft) AS sqft_bit_or,
    GROUPING(seq_no) AS gid
FROM kept_stores
GROUP BY GROUPING SETS ((seq_no, DATE_TRUNC('MONTH', opened_date)), (seq_no), ())
HAVING sqft_bit_and > 1
ORDER BY sqft_bit_and NULLS FIRST
