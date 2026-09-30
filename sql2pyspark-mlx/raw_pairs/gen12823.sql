WITH metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date)), 4) AS monthly_fee_share,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY monthly_fee, id) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY start_date, id) AS seq_no,
        AVG(monthly_fee) OVER (PARTITION BY status ORDER BY start_date, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS monthly_fee_avg_2_prior
    FROM subscriptions
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        sub.id AS subscription_id,
        sub.plan AS subscription_plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM metrics src
    LEFT JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.end_date BETWEEN sub.start_date AND sub.end_date
),
joined AS (
    -- Attach customers attributes
    SELECT
        s.id,
        s.customer_id,
        s.plan,
        s.start_date,
        s.end_date,
        s.monthly_fee,
        s.status,
        s.monthly_fee_share,
        s.row_seq,
        s.subscription_id,
        s.subscription_plan,
        s.subscription_monthly_fee,
        c.segment
    FROM with_window s
    INNER JOIN customers c
        ON s.customer_id = c.id
    WHERE c.segment IN ('SMB', 'Consumer')
),
matched_keys AS (
    -- Set intersection on keys
    SELECT status, customer_id
    FROM joined
    INTERSECT ALL
    SELECT status, customer_id
    FROM subscriptions
    WHERE MONTH(start_date) IN (3, 7, 9)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM joined s
    INNER JOIN matched_keys k ON s.status = k.status AND s.customer_id = k.customer_id
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        a.end_date,
        a.monthly_fee_share,
        a.subscription_monthly_fee,
        p.credit_limit
    FROM kept_rows a
    LEFT JOIN customers p
        ON a.customer_id = p.id
),
metrics_subscriptions AS (
    -- Ranking and gap features
    SELECT
        id,
        monthly_fee_share,
        subscription_monthly_fee,
        credit_limit,
        CUME_DIST() OVER (ORDER BY subscription_monthly_fee DESC) AS subscription_monthly_fee_cume_dist,
        AVG(monthly_fee_share) OVER w_recent AS customer_id_avg_monthly_fee_share,
        MAX(subscription_monthly_fee) OVER (PARTITION BY MONTH(end_date), customer_id) AS end_date_month_max_subscription_monthly_fee
    FROM with_dims
    WINDOW w_recent AS (PARTITION BY customer_id)
)
SELECT
    id,
    monthly_fee_share,
    subscription_monthly_fee,
    credit_limit,
    subscription_monthly_fee_cume_dist,
    customer_id_avg_monthly_fee_share,
    end_date_month_max_subscription_monthly_fee
FROM metrics_subscriptions
ORDER BY subscription_monthly_fee_cume_dist DESC;
