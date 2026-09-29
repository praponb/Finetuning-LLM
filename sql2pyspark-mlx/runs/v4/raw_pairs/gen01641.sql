WITH base AS (
    SELECT
        *,
        CASE WHEN account_type IN ('SAVINGS', 'CHECKING') THEN 'Priority' ELSE 'Other' END AS account_type_group,
        COALESCE(status, 'N/A') AS status_clean
    FROM accounts
    WHERE status IS NOT NULL
),
latest AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY account_type ORDER BY balance DESC NULLS LAST) AS rank_in_group
        FROM base
    ) ranked
    WHERE rank_in_group = 1
),
latest_accounts AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY opened_date NULLS FIRST, id) AS rn
        FROM latest
    ) ranked
    WHERE rn = 1
),
base_accounts AS (
    SELECT *, LEAST(rn, rank_in_group) AS least_rn_rank_in_group
    FROM latest_accounts
    WHERE opened_date >= DATE '2024-03-09'
      AND customer_id IS NOT NULL
),
pivoted AS (
    SELECT *
    FROM (
        SELECT
            customer_id,
            status,
            account_type,
            least_rn_rank_in_group
        FROM base_accounts
    ) src
    PIVOT (
        AVG(least_rn_rank_in_group)
        FOR account_type IN ('CHECKING', 'LOAN')
    )
)
SELECT
    s.customer_id,
    s.status,
    s.CHECKING,
    (SELECT SUM(x.monthly_fee) FROM subscriptions x WHERE x.customer_id = s.customer_id) AS sum_subscription_monthly_fee
FROM pivoted s
ORDER BY CHECKING DESC;
