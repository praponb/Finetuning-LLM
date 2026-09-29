WITH with_dims AS (
    -- Attach accounts attributes
    SELECT
        src.id,
        src.account_id,
        src.txn_time,
        p.account_type
    FROM transactions src
    LEFT JOIN accounts p
        ON src.account_id = p.id
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        account_id,
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY txn_time, id) AS event_seq,
        FIRST_VALUE(account_type, TRUE) OVER (PARTITION BY account_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_account_type,
        LAST_VALUE(account_type) IGNORE NULLS OVER (PARTITION BY account_id, account_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_account_type,
        ROW_NUMBER() OVER (ORDER BY txn_time DESC, id) AS event_seq_2,
        FIRST_VALUE(account_type, TRUE) OVER (PARTITION BY YEAR(txn_time), account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_account_type_2
    FROM with_dims
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(event_seq) OVER w_ordered AS cumulative_event_seq,
        LAST_VALUE(event_seq) IGNORE NULLS OVER w_ordered AS last_known_event_seq
    FROM with_windows
    WHERE event_seq < 4
    WINDOW w_ordered AS (ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(last_known_event_seq * 100, 2) AS adjusted_last_known_event_seq
    FROM scored
    WHERE event_seq_2 <> 7
)
-- Grouped metrics
SELECT
    CASE WHEN cumulative_event_seq >= 39 THEN 'LARGE' ELSE 'SMALL' END AS cumulative_event_seq_tier,
    CASE WHEN event_seq >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS event_seq_tier,
    ROUND(AVG(adjusted_last_known_event_seq), 2) AS avg_adjusted_last_known_event_seq,
    MIN(event_seq_2) AS min_event_seq_2,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN cumulative_event_seq >= 39 THEN 'LARGE' ELSE 'SMALL' END ORDER BY MIN(event_seq_2) DESC) AS min_event_seq_2_percent_rank
FROM valid
GROUP BY 1, 2;
