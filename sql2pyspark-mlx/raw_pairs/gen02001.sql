WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        COALESCE(status, 'unassigned') AS status_filled,
        ADD_MONTHS(opened_date, 12) AS opened_date_plus_months,
        COALESCE(status, 'OTHER') AS status_filled_2
    FROM accounts
    WHERE opened_date > TO_DATE('2023-08-18')
      AND opened_date >= '2024-12-26'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(5) OVER w_part AS balance_bucket,
        SUM(balance) OVER w_hist AS cumulative_balance
    FROM valid
    WINDOW w_part AS (PARTITION BY YEAR(opened_date), status ORDER BY balance DESC, id), w_hist AS (PARTITION BY DATE_TRUNC('WEEK', opened_date_plus_months), customer_id ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM metrics src
    LEFT JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.opened_date_plus_months BETWEEN sub.start_date AND sub.end_date
),
joined AS (
    -- Attach customers attributes
    SELECT
        src.id,
        src.customer_id,
        src.account_type,
        src.status,
        src.status_filled_2,
        src.cumulative_balance,
        src.subscription_id,
        src.plan,
        src.monthly_fee,
        b.credit_limit,
        b.country
    FROM with_window src
    JOIN customers b
        ON src.customer_id <=> b.id
    WHERE b.age >= 52
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY country ORDER BY credit_limit) AS row_num
    FROM joined
    QUALIFY row_num = 1
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        s.id,
        s.customer_id,
        s.status,
        s.subscription_id,
        s.plan,
        s.monthly_fee,
        b.segment
    FROM latest s
    LEFT JOIN customers b
        ON s.customer_id <=> b.id
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_dims s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.is_flagged
        )
),
stats AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN monthly_fee >= 222 THEN 'LARGE' ELSE 'SMALL' END AS monthly_fee_tier,
        AVG(subscription_id) AS avg_subscription_id,
        SIZE(COLLECT_SET(segment)) AS num_segments,
        SIZE(COLLECT_SET(segment)) AS num_segments_2,
        SUM(CASE WHEN segment = 'Corporate' THEN subscription_id ELSE 0 END) AS corporate_subscription_id,
        ROUND(AVG(subscription_id), 2) AS avg_subscription_id_2,
        APPROX_PERCENTILE(subscription_id, ARRAY(0.25, 0.5, 0.75)) AS subscription_id_quartiles
    FROM not_flagged
    GROUP BY CASE WHEN monthly_fee >= 222 THEN 'LARGE' ELSE 'SMALL' END
)
SELECT
    monthly_fee_tier,
    avg_subscription_id,
    num_segments,
    corporate_subscription_id,
    subscription_id_quartiles
FROM stats;
