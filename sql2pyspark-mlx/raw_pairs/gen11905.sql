WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, ADD_MONTHS(start_date, 6) AS start_date_plus_months
    FROM subscriptions
    WHERE end_date > '2025-04-01'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        end_date,
        status,
        start_date_plus_months,
        LAG(monthly_fee) OVER (PARTITION BY plan ORDER BY start_date, id) AS prev_monthly_fee,
        LEAD(monthly_fee) OVER (PARTITION BY plan ORDER BY start_date, id) AS next_monthly_fee,
        ROW_NUMBER() OVER (PARTITION BY plan ORDER BY start_date, id) AS seq_no
    FROM cleaned
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        status,
        prev_monthly_fee,
        next_monthly_fee,
        seq_no,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY seq_no DESC, id) AS event_seq,
        SUM(prev_monthly_fee) OVER (PARTITION BY customer_id, status ORDER BY prev_monthly_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_prev_monthly_fee
    FROM enriched
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        status,
        prev_monthly_fee,
        next_monthly_fee,
        seq_no,
        event_seq,
        running_prev_monthly_fee,
        UPPER(status) AS status_upper
    FROM with_windows
    WHERE status IN ('CANCELLED', 'PAUSED')
),
base AS (
    -- Keep recent, valid rows
    SELECT *, prev_monthly_fee - running_prev_monthly_fee AS prev_monthly_fee_minus_running_prev_monthly_fee
    FROM valid
    WHERE prev_monthly_fee >= 190
),
enriched_subscriptions AS (
    -- Attach customers attributes
    SELECT
        e.*,
        p.segment,
        p.email,
        p.city
    FROM base e
    LEFT JOIN customers p
        ON e.customer_id = p.id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(next_monthly_fee) OVER (PARTITION BY customer_id, status ORDER BY event_seq, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_next_monthly_fee,
        AVG(next_monthly_fee) OVER (PARTITION BY customer_id, status ORDER BY event_seq, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_next_monthly_fee,
        MAX(next_monthly_fee) OVER (PARTITION BY customer_id, status ORDER BY event_seq, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_next_monthly_fee
    FROM enriched_subscriptions
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        status,
        city,
        last4_count_next_monthly_fee
    FROM metrics
) src
PIVOT (
    MAX(last4_count_next_monthly_fee) AS amount,
    COUNT(1) AS cnt
    FOR city IN ('New York' AS new_york, 'Chiang Mai' AS chiang_mai)
)
