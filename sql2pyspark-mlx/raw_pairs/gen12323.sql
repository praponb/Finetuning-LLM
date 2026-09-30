WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        merchant,
        is_flagged,
        ROUND(amount, 2) AS amount_rounded,
        COALESCE(merchant, 'NONE') AS merchant_filled
    FROM transactions
    WHERE amount >= 2300
      AND merchant IS NOT DISTINCT FROM 'Amazon'
),
with_dims AS (
    -- Attach accounts attributes
    SELECT
        src.id,
        src.account_id,
        src.txn_type,
        src.merchant,
        src.is_flagged,
        p.account_type,
        p.balance,
        p.opened_date
    FROM valid src
    LEFT JOIN accounts p
        ON src.account_id = p.id
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY account_type, txn_type ORDER BY balance) AS row_num
        FROM with_dims
    ) ranked
    WHERE row_num = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(account_type, TRUE) OVER (ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_account_type,
        FIRST_VALUE(account_type) IGNORE NULLS OVER (PARTITION BY account_type ORDER BY row_num, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_account_type_2,
        AVG(balance) OVER w_hist AS balance_avg_3_prior
    FROM deduped
    WINDOW w_hist AS (ORDER BY opened_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING)
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        account_id,
        txn_type,
        merchant,
        is_flagged,
        account_type,
        first_known_account_type,
        LAG(balance, 2) OVER (PARTITION BY txn_type ORDER BY balance, id) AS prev_balance,
        AVG(balance_avg_3_prior) OVER (ORDER BY balance, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS balance_avg_3_prior_avg_5_prior
    FROM enriched
    WHERE opened_date BETWEEN '2025-05-01' AND '2025-06-01'
),
deduped_transactions AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        merchant,
        account_type,
        prev_balance
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY prev_balance DESC, id) = 1
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT(account_type, ', ', merchant) AS account_type_merchant,
        COALESCE(account_type, 'OTHER') AS resolved_account_type
    FROM deduped_transactions
    WHERE account_type IS NOT NULL
)
SELECT
    id,
    merchant,
    account_type,
    account_type_merchant,
    resolved_account_type
FROM base
ORDER BY id DESC NULLS FIRST;
