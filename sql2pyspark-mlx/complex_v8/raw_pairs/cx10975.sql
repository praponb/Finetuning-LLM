WITH cleaned AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        DATEDIFF(DATE '2026-01-01', start_date) AS days_since_start_date
    FROM policies
    WHERE (product <> 'LIFE' OR premium BETWEEN 1042.5 AND 3465.4)
),
combined_details AS (
    -- Attach holder attributes
    SELECT s.*, p.holder_name, p.joined_on AS holder_joined_on
    FROM cleaned s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER (PARTITION BY status ORDER BY start_date DESC, id) AS start_date_rank,
        ROUND(PERCENT_RANK() OVER w_product_by_start_date, 3) AS start_date_percent_rank,
        DATEDIFF(holder_joined_on, LAG(holder_joined_on) OVER (PARTITION BY product ORDER BY start_date ASC NULLS LAST, id)) AS days_since_prev_holder_joined_on,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY start_date DESC, id) AS start_date_seq,
        RANK() OVER (PARTITION BY product ORDER BY start_date ASC NULLS LAST, id) AS start_date_rank_2,
        ROUND(premium - AVG(premium) OVER w_status, 2) AS premium_vs_status_avg,
        FIRST_VALUE(start_date) IGNORE NULLS OVER (PARTITION BY status ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_start_date
    FROM combined_details
    WINDOW w_product_by_start_date AS (PARTITION BY product ORDER BY start_date ASC NULLS LAST, id), w_status AS (PARTITION BY status)
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY start_date_rank_2 ORDER BY days_since_prev_holder_joined_on), 3) AS days_since_prev_holder_joined_on_percent_rank,
        COUNT(*) OVER (PARTITION BY start_date_seq, MONTH(first_known_start_date)) AS rows_in_start_date_seq_first_known_start_date_mo,
        DENSE_RANK() OVER w_all_by_start_date_percent_ra AS start_date_percent_rank_dense_rank
    FROM windowed
    WINDOW w_start_date_seq_first_known_s AS (PARTITION BY start_date_seq, MONTH(first_known_start_date)), w_all_by_start_date_percent_ra AS (ORDER BY start_date_percent_rank ASC NULLS LAST)
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    start_date,
    end_date,
    status,
    days_since_start_date,
    holder_joined_on,
    start_date_rank,
    days_since_prev_holder_joined_on,
    start_date_seq,
    start_date_rank_2,
    premium_vs_status_avg,
    first_known_start_date,
    start_date_percent_rank_dense_rank,
    LOWER(REGEXP_REPLACE(TRIM(holder_name), ' ', '_')) AS holder_name_slug
FROM with_windows
WHERE start_date_rank_2 <= 9
ORDER BY days_since_start_date DESC, start_date DESC NULLS FIRST, id
LIMIT 5
