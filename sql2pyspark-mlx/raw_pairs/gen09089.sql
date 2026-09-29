WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        DATEDIFF(CURRENT_DATE(), end_date) AS days_since_end_date
    FROM subscriptions
    WHERE start_date > DATE '2023-06-07'
      AND customer_id IS NOT NULL
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY days_since_end_date DESC NULLS LAST) AS days_since_end_date_dense_rank,
        LAST_VALUE(days_since_end_date) IGNORE NULLS OVER (PARTITION BY customer_id ORDER BY days_since_end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_days_since_end_date,
        SUM(monthly_fee) OVER (PARTITION BY plan ORDER BY days_since_end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_monthly_fee
    FROM base
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        end_date,
        monthly_fee,
        status,
        days_since_end_date,
        last_known_days_since_end_date,
        cumulative_monthly_fee
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY plan, status ORDER BY days_since_end_date NULLS LAST, id) = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER w_ordered AS seq_no,
        NTILE(5) OVER w_ordered AS monthly_fee_bucket
    FROM latest
    WINDOW w_ordered AS (PARTITION BY status ORDER BY monthly_fee, id)
)
SELECT
    id,
    status,
    seq_no
FROM with_windows
ORDER BY seq_no
