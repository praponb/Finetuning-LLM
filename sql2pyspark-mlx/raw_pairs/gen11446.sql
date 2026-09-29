WITH base AS (
    SELECT *, DATEDIFF(CURRENT_DATE(), opened_date) / 7 weeks_since_opened_date
    FROM accounts
    WHERE opened_date >= DATE '2023-05-26'
      AND customer_id IS NOT NULL
),
enriched AS (
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        SUM(weeks_since_opened_date) OVER w_grp cumulative_weeks_since_opened_date,
        SUM(weeks_since_opened_date) OVER w_recent running_weeks_since_opened_date,
        SUM(weeks_since_opened_date) OVER (PARTITION BY account_type) account_type_sum_weeks_since_opened_date
    FROM base
    WINDOW w_grp AS (PARTITION BY YEAR(opened_date), status ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY DATE_TRUNC('week', opened_date), status ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
kept_keys AS (
    SELECT id, customer_id
    FROM enriched
    EXCEPT ALL
    SELECT id, customer_id
    FROM accounts
    WHERE opened_date < '2023-07-01'
),
kept_rows AS (
    SELECT s.*
    FROM enriched s
    INNER JOIN kept_keys k ON s.id = k.id AND s.customer_id = k.customer_id
),
with_windows AS (
    SELECT
        id,
        balance,
        opened_date,
        AVG(balance) OVER (PARTITION BY account_type) account_type_avg_balance,
        DENSE_RANK() OVER (PARTITION BY YEAR(opened_date), customer_id ORDER BY cumulative_weeks_since_opened_date DESC) cumulative_weeks_since_opened_date_dense_rank,
        SUM(balance) OVER (PARTITION BY account_type, customer_id) account_type_sum_balance,
        MIN(cumulative_weeks_since_opened_date) OVER (PARTITION BY account_type) account_type_min_cumulative_weeks_since_opened_date
    FROM kept_rows
),
not_flagged AS (
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time >= s.opened_date
        )
),
not_flagged_accounts AS (
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time >= s.opened_date
        )
),
enriched_accounts AS (
    SELECT
        id,
        account_type_avg_balance,
        cumulative_weeks_since_opened_date_dense_rank,
        account_type_sum_balance,
        account_type_min_cumulative_weeks_since_opened_date,
        PERCENT_RANK() OVER (ORDER BY account_type_min_cumulative_weeks_since_opened_date DESC NULLS FIRST) account_type_min_cumulative_weeks_since_opened_date_percent_rank,
        CUME_DIST() OVER (ORDER BY cumulative_weeks_since_opened_date_dense_rank DESC NULLS LAST) cumulative_weeks_since_opened_date_dense_rank_cume_dist,
        CUME_DIST() OVER (ORDER BY cumulative_weeks_since_opened_date_dense_rank) cumulative_weeks_since_opened_date_dense_rank_cume_dist_2
    FROM not_flagged_accounts
)
SELECT
    id,
    cumulative_weeks_since_opened_date_dense_rank,
    account_type_min_cumulative_weeks_since_opened_date,
    account_type_min_cumulative_weeks_since_opened_date_percent_rank,
    cumulative_weeks_since_opened_date_dense_rank_cume_dist_2,
    RANK() OVER (ORDER BY account_type_min_cumulative_weeks_since_opened_date_percent_rank NULLS FIRST) account_type_min_cumulative_weeks_since_opened_date_percent_rank_rank,
    CUME_DIST() OVER (ORDER BY cumulative_weeks_since_opened_date_dense_rank NULLS LAST) cumulative_weeks_since_opened_date_dense_rank_cume_dist_3
FROM enriched_accounts;
