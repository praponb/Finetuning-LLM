WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id, status ORDER BY end_date NULLS LAST, id) AS rn
    FROM subscriptions
    QUALIFY rn = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        plan,
        end_date,
        NTILE(10) OVER (PARTITION BY plan, customer_id ORDER BY monthly_fee DESC, id) AS monthly_fee_bucket,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY YEAR(end_date), status), 4) AS monthly_fee_share,
        COUNT(monthly_fee) OVER (PARTITION BY status) AS status_count_monthly_fee
    FROM latest
    WHERE monthly_fee >= 180
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        end_date,
        monthly_fee_share,
        LEAD(monthly_fee_share) OVER w_running AS next_monthly_fee_share,
        LEAD(monthly_fee_bucket) OVER w_running AS next_monthly_fee_bucket,
        ROW_NUMBER() OVER w_running AS row_seq,
        AVG(monthly_fee_bucket) OVER (PARTITION BY plan ORDER BY end_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS monthly_fee_bucket_avg_6
    FROM with_windows
    WINDOW w_running AS (PARTITION BY plan ORDER BY monthly_fee_bucket, id)
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        end_date,
        next_monthly_fee_share,
        row_seq,
        next_monthly_fee_share / NULLIF(monthly_fee_share, 0) AS next_monthly_fee_share_per_monthly_fee_share
    FROM scored
    WHERE next_monthly_fee_bucket > 8
)
SELECT
    id,
    end_date,
    next_monthly_fee_share,
    row_seq,
    next_monthly_fee_share_per_monthly_fee_share
FROM base
ORDER BY end_date, next_monthly_fee_share_per_monthly_fee_share;
