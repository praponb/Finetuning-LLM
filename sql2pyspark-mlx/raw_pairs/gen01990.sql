WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        balance,
        opened_date,
        status,
        DATEDIFF(CURRENT_DATE(), opened_date) / 30 AS months_since_opened_date,
        COALESCE(status, 'UNKNOWN') AS resolved_status
    FROM accounts
    WHERE opened_date > DATE '2023-04-13'
      AND status IS NOT NULL
),
enriched AS (
    -- Attach customers attributes
    SELECT src.*, p.country
    FROM cleaned src
    JOIN customers p
        ON src.customer_id = p.id
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        balance,
        opened_date,
        months_since_opened_date,
        country,
        LEAD(months_since_opened_date, 2) OVER (ORDER BY months_since_opened_date, id) AS next_months_since_opened_date,
        CUME_DIST() OVER (PARTITION BY status ORDER BY months_since_opened_date DESC) AS months_since_opened_date_cume_dist
    FROM enriched
    WHERE customer_id = 7
),
enriched_accounts AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        opened_date,
        country,
        months_since_opened_date_cume_dist,
        ROW_NUMBER() OVER (ORDER BY months_since_opened_date, id) AS event_seq,
        MAX(months_since_opened_date_cume_dist) OVER (PARTITION BY MONTH(opened_date), country) AS opened_date_month_max_months_since_opened_date_cume_dist,
        PERCENT_RANK() OVER (PARTITION BY country ORDER BY next_months_since_opened_date DESC) AS next_months_since_opened_date_percent_rank
    FROM with_windows
),
cleaned_accounts AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        event_seq * months_since_opened_date_cume_dist AS event_seq_times_months_since_opened_date_cume_dist,
        COALESCE(country, 'NONE') AS resolved_country
    FROM enriched_accounts
    WHERE opened_date > DATE '2023-08-15'
),
with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.country,
        s.months_since_opened_date_cume_dist,
        s.event_seq,
        s.next_months_since_opened_date_percent_rank,
        s.resolved_country,
        x.transaction_txn_type
    FROM cleaned_accounts s
    JOIN LATERAL (
            SELECT c.txn_type AS transaction_txn_type
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_windows_accounts AS (
    -- Ranking and gap features
    SELECT
        id,
        months_since_opened_date_cume_dist,
        event_seq,
        resolved_country,
        transaction_txn_type,
        MIN(event_seq) OVER (PARTITION BY transaction_txn_type, country ORDER BY months_since_opened_date_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_event_seq,
        COUNT(event_seq) OVER (PARTITION BY transaction_txn_type, country ORDER BY months_since_opened_date_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_event_seq,
        SUM(event_seq) OVER (PARTITION BY transaction_txn_type, country ORDER BY months_since_opened_date_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_event_seq,
        MAX(event_seq) OVER (PARTITION BY transaction_txn_type, country ORDER BY months_since_opened_date_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_event_seq,
        PERCENT_RANK() OVER (ORDER BY event_seq DESC) AS event_seq_percent_rank
    FROM with_last
)
SELECT
    id,
    months_since_opened_date_cume_dist,
    event_seq,
    resolved_country,
    transaction_txn_type,
    running_max_event_seq,
    CASE WHEN transaction_txn_type RLIKE '^(CREDIT|REFUND)$' THEN 'EXTENDED' WHEN transaction_txn_type RLIKE '^(DEBIT|FEE)$' THEN 'GROUP_A' ELSE 'CORE' END AS transaction_txn_type_zone
FROM with_windows_accounts
ORDER BY transaction_txn_type_zone DESC;
