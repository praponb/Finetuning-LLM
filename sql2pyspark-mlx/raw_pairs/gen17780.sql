WITH joined AS (
    -- Attach accounts attributes
    SELECT
        a.id,
        a.account_id,
        a.txn_type,
        a.amount,
        a.is_flagged,
        b.opened_date
    FROM transactions a
    LEFT JOIN accounts b
        ON a.account_id = b.id
),
enriched AS (
    -- Attach accounts attributes
    SELECT
        s.id,
        s.account_id,
        s.is_flagged,
        s.opened_date,
        p.account_type
    FROM joined s
    INNER JOIN accounts p
        ON s.account_id = p.id
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        is_flagged,
        account_type,
        ROW_NUMBER() OVER w_recent AS event_seq,
        ROW_NUMBER() OVER w_recent AS seq_no,
        LAST_VALUE(account_type) IGNORE NULLS OVER (PARTITION BY account_id ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_account_type,
        FIRST_VALUE(account_type) IGNORE NULLS OVER (ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_account_type,
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY opened_date, id) AS event_seq_2
    FROM enriched
    WINDOW w_recent AS (PARTITION BY account_type ORDER BY opened_date, id)
),
enriched_transactions AS (
    -- Running and moving aggregates
    SELECT
        id,
        account_type,
        event_seq,
        last_known_account_type,
        event_seq_2,
        AVG(event_seq_2) OVER (ORDER BY event_seq, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS event_seq_2_avg_2,
        FIRST_VALUE(event_seq_2, TRUE) OVER (PARTITION BY account_type ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_seq_2
    FROM scored
    WHERE event_seq_2 BETWEEN 4 AND 8
),
scored_transactions AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY account_type ORDER BY event_seq, id) AS event_seq_bucket,
        LEAD(event_seq_2_avg_2, 1) OVER (ORDER BY first_known_event_seq_2, id) AS next_event_seq_2_avg_2,
        SUM(event_seq_2_avg_2) OVER (PARTITION BY account_type) AS account_type_sum_event_seq_2_avg_2
    FROM enriched_transactions
)
SELECT
    id,
    event_seq,
    event_seq_2,
    first_known_event_seq_2,
    event_seq_bucket
FROM scored_transactions
ORDER BY id, event_seq
LIMIT 10
