WITH scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(monthly_fee, 2) OVER (PARTITION BY status ORDER BY monthly_fee, id) AS prev_monthly_fee,
        NTILE(3) OVER (ORDER BY monthly_fee, id) AS monthly_fee_bucket,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY start_date DESC, id) AS row_seq
    FROM subscriptions
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT plan, status
    FROM scored
    INTERSECT
    SELECT plan, status
    FROM subscriptions
    WHERE status != 'EXPIRED'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN remaining_keys k ON s.plan = k.plan AND s.status = k.status
),
scored_subscriptions AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY row_seq NULLS LAST) AS row_seq_percent_rank,
        DENSE_RANK() OVER (PARTITION BY customer_id, plan ORDER BY monthly_fee NULLS FIRST) AS monthly_fee_dense_rank,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY status), 4) AS monthly_fee_share
    FROM kept_rows
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        prev_monthly_fee,
        monthly_fee_bucket,
        row_seq,
        row_seq_percent_rank,
        monthly_fee_dense_rank,
        monthly_fee_share,
        ROUND(monthly_fee_dense_rank / SUM(monthly_fee_dense_rank) OVER (PARTITION BY customer_id), 4) AS monthly_fee_dense_rank_share,
        CUME_DIST() OVER (PARTITION BY customer_id ORDER BY monthly_fee_share DESC) AS monthly_fee_share_cume_dist,
        DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY monthly_fee_share DESC NULLS FIRST) AS monthly_fee_share_dense_rank
    FROM scored_subscriptions
)
SELECT
    customer_id,
    end_date,
    status,
    prev_monthly_fee,
    monthly_fee_bucket,
    row_seq,
    row_seq_percent_rank,
    monthly_fee_dense_rank,
    monthly_fee_share
FROM with_windows;
