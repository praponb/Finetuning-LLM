WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY plan ORDER BY monthly_fee, id) AS monthly_fee_bucket,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY plan, customer_id), 4) AS monthly_fee_share,
        LEAD(monthly_fee, 1) OVER (PARTITION BY customer_id ORDER BY monthly_fee, id) AS next_monthly_fee
    FROM subscriptions
),
kept_keys AS (
    -- Set intersection on keys
    SELECT status
    FROM scored
    INTERSECT
    SELECT status
    FROM subscriptions
    WHERE monthly_fee IS NULL
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    JOIN kept_keys k ON s.status = k.status
),
scored_subscriptions AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY plan, customer_id ORDER BY next_monthly_fee DESC NULLS LAST) AS next_monthly_fee_percent_rank,
        PERCENT_RANK() OVER (PARTITION BY customer_id ORDER BY monthly_fee_bucket DESC NULLS FIRST) AS monthly_fee_bucket_percent_rank,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY status), 4) AS monthly_fee_share_2,
        ROUND(monthly_fee_bucket / SUM(monthly_fee_bucket) OVER (PARTITION BY plan), 4) AS monthly_fee_bucket_share,
        ROUND(next_monthly_fee / SUM(next_monthly_fee) OVER (PARTITION BY plan), 4) AS next_monthly_fee_share
    FROM reconciled
),
kept_keys_2 AS (
    -- Set difference on keys
    SELECT customer_id
    FROM scored_subscriptions
    EXCEPT ALL
    SELECT customer_id
    FROM subscriptions
    WHERE plan <> 'standard'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored_subscriptions s
    JOIN kept_keys_2 k ON s.customer_id = k.customer_id
),
matched_keys AS (
    -- Set difference on keys
    SELECT customer_id
    FROM verified
    EXCEPT
    SELECT customer_id
    FROM subscriptions
    WHERE monthly_fee BETWEEN 170 AND 240
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM verified s
    JOIN matched_keys k ON s.customer_id = k.customer_id
),
with_dims AS (
    -- Attach customers attributes
    SELECT e.*, p.segment
    FROM kept_rows e
    LEFT JOIN customers p
        ON e.customer_id = p.id
)
SELECT
    id,
    customer_id,
    plan,
    start_date,
    end_date,
    status,
    monthly_fee_bucket,
    monthly_fee_share,
    next_monthly_fee,
    CASE WHEN status RLIKE '^(EXPIRED|ACTIVE)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS status_zone
FROM with_dims
ORDER BY start_date NULLS LAST;
