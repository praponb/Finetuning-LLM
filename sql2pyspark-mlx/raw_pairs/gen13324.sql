WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY customer_id), 4) AS monthly_fee_share,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY monthly_fee NULLS FIRST) AS monthly_fee_percent_rank,
        CUME_DIST() OVER (ORDER BY monthly_fee DESC) AS monthly_fee_cume_dist
    FROM subscriptions
),
enriched AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        a.monthly_fee,
        a.monthly_fee_share,
        a.monthly_fee_percent_rank,
        d.segment,
        d.age
    FROM with_windows a
    LEFT JOIN customers d
        ON a.customer_id = d.id
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        monthly_fee / NULLIF(monthly_fee_share, 0) AS monthly_fee_per_monthly_fee_share,
        COALESCE(segment, 'OTHER') AS segment_clean
    FROM enriched
    WHERE customer_id IS NOT NULL
      AND segment IS NOT DISTINCT FROM 'SMB'
),
enriched_subscriptions AS (
    -- Attach customers attributes
    SELECT
        src.*,
        b.signup_date,
        b.email,
        b.credit_limit
    FROM base src
    JOIN customers b
        ON src.customer_id = b.id
    WHERE b.country = 'DE'
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LAST_DAY(signup_date) AS signup_date_month_end,
        COALESCE(segment, 'NONE') AS segment_filled
    FROM enriched_subscriptions
    WHERE segment IS NOT NULL
      AND segment IS DISTINCT FROM 'Consumer'
),
valid_subscriptions AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        MONTH(signup_date_month_end) AS signup_date_month_end_month,
        monthly_fee_share - monthly_fee AS monthly_fee_share_minus_monthly_fee
    FROM valid
    WHERE segment IS NOT DISTINCT FROM 'Corporate'
      AND monthly_fee_per_monthly_fee_share > 71
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM valid_subscriptions a
    LEFT JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.signup_date BETWEEN sub.start_date AND sub.end_date
),
totals AS (
    -- Grouped metrics
    SELECT
        DATE_TRUNC('MONTH', signup_date_month_end) AS activity_month,
        customer_id,
        COUNT_IF(segment = 'SMB') AS smb_events,
        SIZE(COLLECT_SET(segment)) AS num_segments,
        COUNT(DISTINCT customer_id) AS distinct_customer_ids
    FROM matched
    GROUP BY activity_month, customer_id
)
SELECT
    s.activity_month,
    s.customer_id,
    s.smb_events,
    s.num_segments,
    (SELECT SUM(x.balance) FROM accounts x WHERE x.customer_id = s.customer_id) AS sum_account_balance
FROM totals s
ORDER BY smb_events
