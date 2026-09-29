WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(monthly_fee) OVER w_running AS last7_count_monthly_fee,
        MAX(monthly_fee) OVER w_running AS last7_max_monthly_fee,
        MIN(monthly_fee) OVER w_running AS last7_min_monthly_fee,
        NTILE(10) OVER (PARTITION BY customer_id, status ORDER BY monthly_fee, id) AS monthly_fee_bucket
    FROM subscriptions
    WINDOW w_running AS (PARTITION BY customer_id ORDER BY end_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        plan,
        end_date,
        status,
        last7_count_monthly_fee,
        last7_max_monthly_fee,
        last7_min_monthly_fee,
        monthly_fee_bucket,
        CAST(last7_count_monthly_fee AS STRING) AS last7_count_monthly_fee_num
    FROM scored
    WHERE end_date >= TO_DATE('2023-07-28')
),
kept_keys AS (
    -- Set intersection on keys
    SELECT customer_id, plan
    FROM valid
    INTERSECT
    SELECT customer_id, plan
    FROM subscriptions
    WHERE MONTH(end_date) IN (3, 5, 11)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN kept_keys k ON s.customer_id = k.customer_id AND s.plan = k.plan
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        src.customer_id,
        src.plan,
        src.status,
        src.last7_count_monthly_fee,
        src.last7_min_monthly_fee,
        src.monthly_fee_bucket,
        src.last7_count_monthly_fee_num,
        d.credit_limit,
        d.age,
        d.city
    FROM kept_rows src
    LEFT JOIN customers d
        ON src.customer_id <=> d.id
),
scored_subscriptions AS (
    -- Window metrics over several different partitions
    SELECT
        customer_id,
        status,
        last7_count_monthly_fee,
        monthly_fee_bucket,
        last7_count_monthly_fee_num,
        age,
        MAX(age) OVER (PARTITION BY customer_id) AS customer_id_max_age,
        MIN(monthly_fee_bucket) OVER (PARTITION BY customer_id) AS customer_id_min_monthly_fee_bucket,
        PERCENT_RANK() OVER (PARTITION BY customer_id ORDER BY last7_min_monthly_fee DESC NULLS FIRST) AS last7_min_monthly_fee_percent_rank,
        DENSE_RANK() OVER (PARTITION BY city ORDER BY age) AS age_dense_rank
    FROM with_dims
)
SELECT
    s.customer_id,
    s.status,
    s.last7_count_monthly_fee,
    s.monthly_fee_bucket,
    s.age,
    s.customer_id_min_monthly_fee_bucket,
    s.last7_min_monthly_fee_percent_rank,
    s.age_dense_rank,
    (SELECT SUM(t2.balance) FROM accounts t2 WHERE t2.customer_id = s.customer_id) AS sum_account_balance
FROM scored_subscriptions s;
