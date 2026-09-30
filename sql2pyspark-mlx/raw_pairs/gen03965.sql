WITH enriched AS (
    -- Attach customers attributes
    SELECT
        src.*,
        b.name,
        b.city
    FROM accounts src
    LEFT JOIN customers b
        ON src.customer_id = b.id
),
enriched_accounts AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        opened_date,
        name,
        SUM(balance) OVER (PARTITION BY account_type ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance,
        RANK() OVER (PARTITION BY city ORDER BY balance NULLS FIRST) AS balance_rank
    FROM enriched
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        opened_date,
        LAG(running_balance, 2) OVER (PARTITION BY YEAR(opened_date), customer_id ORDER BY running_balance, id) AS prev_running_balance,
        ROW_NUMBER() OVER w_grp AS event_seq,
        SUM(balance_rank) OVER w_running AS running_balance_rank
    FROM enriched_accounts
    WINDOW w_grp AS (ORDER BY balance_rank, id), w_running AS (PARTITION BY YEAR(opened_date), customer_id ORDER BY running_balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM scored
    EXCEPT
    SELECT id
    FROM accounts
    WHERE status IN ('FROZEN', 'CLOSED')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    JOIN remaining_keys k ON s.id = k.id
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN running_balance_rank >= 32 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_balance_rank_tier,
    DATE_TRUNC('QUARTER', opened_date) AS quarter_start,
    MIN(prev_running_balance) AS min_prev_running_balance,
    MIN(prev_running_balance) AS min_prev_running_balance_2,
    PERCENTILE_APPROX(event_seq, ARRAY(0.25, 0.5, 0.75)) AS event_seq_quartiles,
    GROUPING(CASE WHEN running_balance_rank >= 32 THEN 'PREMIUM' ELSE 'STANDARD' END) AS grouping_level
FROM kept_rows
GROUP BY CUBE (CASE WHEN running_balance_rank >= 32 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('QUARTER', opened_date))
ORDER BY min_prev_running_balance_2 DESC;
