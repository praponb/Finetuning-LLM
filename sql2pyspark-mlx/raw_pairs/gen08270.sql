WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        account_type,
        balance,
        opened_date,
        status,
        CASE WHEN status IN ('CLOSED', 'FROZEN') THEN 'Group A' ELSE 'Other' END AS status_group
    FROM accounts
    WHERE opened_date >= DATE '2023-05-29'
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.account_id = 32
        )
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT_WS('-', account_type, status) AS account_type_status_key,
        COALESCE(status, 'N/A') AS status_filled
    FROM eligible
    WHERE opened_date > DATE '2023-10-01'
      AND status IS NOT NULL
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER w_grp AS balance_dense_rank,
        AVG(balance) OVER w_recent AS balance_avg_6_prior
    FROM filtered
    WINDOW w_grp AS (ORDER BY balance DESC NULLS FIRST), w_recent AS (PARTITION BY account_type, status ORDER BY opened_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING)
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY opened_date DESC, id) AS row_num
        FROM metrics
    ) t
    WHERE row_num = 1
),
base_accounts AS (
    -- Keep recent, valid rows
    SELECT
        id,
        account_type,
        opened_date,
        status,
        account_type_status_key,
        status_filled,
        balance_avg_6_prior,
        row_num,
        FLOOR(balance_avg_6_prior) AS balance_avg_6_prior_whole,
        ROUND(row_num * 100, 2) AS row_num_scaled
    FROM latest
    WHERE opened_date > TO_DATE('2023-09-01')
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        account_type,
        status,
        row_num_scaled
    FROM base_accounts
) src
PIVOT (
    SUM(row_num_scaled)
    FOR status IN ('CLOSED', 'FROZEN')
)
