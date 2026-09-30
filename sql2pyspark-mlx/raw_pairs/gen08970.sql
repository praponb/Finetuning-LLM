WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CAST(amount AS INT) AS amount_int,
        COALESCE(merchant, 'UNKNOWN') AS merchant_clean
    FROM transactions
    WHERE merchant IS NOT NULL
),
enriched AS (
    -- Attach accounts attributes
    SELECT
        e.*,
        p.status,
        p.account_type
    FROM filtered e
    LEFT JOIN accounts p
        ON e.account_id = p.id
),
enriched_transactions AS (
    -- Attach accounts attributes
    SELECT
        s.*,
        a.account_type AS account_account_type,
        a.opened_date,
        a.status AS account_status
    FROM enriched s
    LEFT JOIN accounts a
        ON s.account_id = a.id
),
enriched_transactions_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        txn_type,
        amount,
        merchant,
        is_flagged,
        account_type,
        account_account_type,
        opened_date,
        account_status,
        AVG(amount_int) OVER (PARTITION BY merchant, status ORDER BY amount, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS amount_int_avg_6_prior,
        AVG(amount) OVER (PARTITION BY account_status ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS amount_avg_90d
    FROM enriched_transactions
    WHERE is_flagged = TRUE
),
enriched_transactions_3 AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(amount) OVER (PARTITION BY account_type ORDER BY amount_avg_90d, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS amount_avg_5_prior,
        SUM(amount) OVER (PARTITION BY account_status, account_type ORDER BY amount_int_avg_6_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount
    FROM enriched_transactions_2
),
enriched_transactions_4 AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(amount_int_avg_6_prior) OVER w_recent AS running_avg_amount_int_avg_6_prior,
        SUM(amount_int_avg_6_prior) OVER w_recent AS running_sum_amount_int_avg_6_prior,
        COUNT(amount_int_avg_6_prior) OVER w_recent AS running_count_amount_int_avg_6_prior,
        AVG(amount) OVER (PARTITION BY account_status ORDER BY amount_int_avg_6_prior, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS amount_avg_5
    FROM enriched_transactions_3
    WINDOW w_recent AS (PARTITION BY account_type ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(amount_avg_90d) OVER w_recent AS running_amount_avg_90d,
        NTILE(3) OVER (PARTITION BY YEAR(opened_date), txn_type ORDER BY amount_int_avg_6_prior DESC, id) AS amount_int_avg_6_prior_bucket,
        CUME_DIST() OVER (PARTITION BY account_type, txn_type ORDER BY amount_avg_5_prior DESC NULLS LAST) AS amount_avg_5_prior_cume_dist
    FROM enriched_transactions_4
    WINDOW w_recent AS (PARTITION BY txn_type ORDER BY cumulative_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
pivoted AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            account_type,
            txn_type,
            amount_avg_90d
        FROM metrics
    ) src
    PIVOT (
        SUM(amount_avg_90d)
        FOR txn_type IN ('CREDIT' AS credit, 'DEBIT' AS debit)
    )
)
SELECT
    account_type,
    credit,
    CASE WHEN account_type RLIKE '^(CHECKING|SAVINGS)$' THEN 'OTHER_ZONE' WHEN account_type RLIKE '^(CREDIT|LOAN)$' THEN 'PRIORITY' ELSE 'GROUP_A' END AS account_type_zone
FROM pivoted
ORDER BY credit DESC NULLS LAST
