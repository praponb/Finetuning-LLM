WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        account_type,
        balance,
        opened_date,
        status,
        INITCAP(TRIM(account_type)) AS account_type_title,
        COALESCE(status, 'DIRECT') AS status_clean
    FROM accounts
    WHERE status IS DISTINCT FROM 'FROZEN'
),
kept_keys AS (
    -- Set difference on keys
    SELECT status, account_type
    FROM base
    EXCEPT
    SELECT status, account_type
    FROM accounts
    WHERE status = 'CLOSED'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN kept_keys k ON s.status = k.status AND s.account_type = k.account_type
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER w_part AS balance_cume_dist,
        RANK() OVER (PARTITION BY status ORDER BY balance DESC NULLS FIRST) AS balance_rank,
        RANK() OVER w_part_2 AS balance_rank_2,
        RANK() OVER (PARTITION BY YEAR(opened_date), status ORDER BY balance DESC) AS balance_rank_3
    FROM verified
    WINDOW w_part AS (PARTITION BY YEAR(opened_date), status ORDER BY balance NULLS LAST), w_part_2 AS (ORDER BY balance DESC NULLS LAST)
),
scored AS (
    -- Ranking and gap features
    SELECT *, ROUND(balance_rank_2 / SUM(balance_rank_2) OVER (PARTITION BY account_type), 4) AS balance_rank_2_share
    FROM enriched
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        account_type,
        opened_date,
        status,
        account_type_title,
        status_clean,
        balance_rank,
        balance_rank_2,
        balance_rank_3,
        balance_rank_2_share,
        STDDEV_SAMP(balance) OVER (PARTITION BY status) AS status_stddev_samp_balance
    FROM scored
    WHERE balance_rank <= 6
),
enriched_accounts AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY status ORDER BY status_stddev_samp_balance DESC) AS status_stddev_samp_balance_dense_rank,
        COUNT(status_stddev_samp_balance) OVER (PARTITION BY account_type) AS account_type_count_status_stddev_samp_balance,
        RANK() OVER (PARTITION BY account_type ORDER BY balance_rank_3 DESC NULLS LAST) AS balance_rank_3_rank
    FROM metrics
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', opened_date) AS activity_month,
        DATE_TRUNC('WEEK', opened_date) AS week_start,
        COUNT(*) AS num_rows,
        SIZE(COLLECT_SET(status)) AS num_status,
        APPROX_PERCENTILE(balance_rank_2_share, 0.95) AS balance_rank_2_share_p95
    FROM enriched_accounts
    GROUP BY DATE_TRUNC('MONTH', opened_date), DATE_TRUNC('WEEK', opened_date)
)
SELECT
    activity_month,
    week_start,
    num_rows,
    num_status,
    balance_rank_2_share_p95
FROM stats
ORDER BY num_status NULLS LAST;
