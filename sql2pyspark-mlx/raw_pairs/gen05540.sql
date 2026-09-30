WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(credit_limit) OVER (ORDER BY age, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS credit_limit_avg_6,
        LAST_VALUE(age) IGNORE NULLS OVER (ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_age
    FROM customers
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(last_known_age) OVER (PARTITION BY segment ORDER BY signup_date, id) AS next_last_known_age,
        LAG(credit_limit) OVER (PARTITION BY segment ORDER BY signup_date, id) AS prev_credit_limit,
        LEAD(credit_limit) OVER (PARTITION BY segment ORDER BY signup_date, id) AS next_credit_limit,
        ROW_NUMBER() OVER (PARTITION BY segment ORDER BY signup_date, id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY YEAR(signup_date), country ORDER BY signup_date, id) AS event_seq
    FROM with_windows
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(credit_limit) OVER w_part AS prev_credit_limit_2,
        LAG(age) OVER w_part AS prev_age,
        ROW_NUMBER() OVER w_part AS event_seq_2,
        DENSE_RANK() OVER (PARTITION BY country, city ORDER BY next_credit_limit NULLS FIRST) AS next_credit_limit_dense_rank
    FROM scored
    WINDOW w_part AS (PARTITION BY country ORDER BY prev_credit_limit, id)
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        age,
        last_known_age,
        prev_credit_limit,
        next_credit_limit,
        seq_no,
        event_seq,
        prev_credit_limit_2,
        event_seq_2,
        CEIL(prev_credit_limit_2) AS prev_credit_limit_2_whole,
        INITCAP(TRIM(city)) AS city_title,
        COALESCE(segment, 'NONE') AS resolved_segment
    FROM enriched
    WHERE signup_date > DATE '2023-09-24'
),
with_windows_customers AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        signup_date,
        age,
        last_known_age,
        prev_credit_limit,
        seq_no,
        event_seq_2,
        prev_credit_limit_2_whole,
        city_title,
        resolved_segment,
        AVG(event_seq_2) OVER w_grp AS running_avg_event_seq_2,
        COUNT(event_seq_2) OVER w_grp AS running_count_event_seq_2,
        MAX(event_seq_2) OVER w_grp AS running_max_event_seq_2
    FROM cleaned
    WINDOW w_grp AS (PARTITION BY segment ORDER BY next_credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_last AS (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.last_known_age,
        s.seq_no,
        s.prev_credit_limit_2_whole,
        s.city_title,
        s.resolved_segment,
        s.running_max_event_seq_2,
        x.status,
        x.account_account_type
    FROM with_windows_customers s
    LEFT JOIN LATERAL (
            SELECT c.status, c.account_type AS account_account_type
            FROM accounts c
            WHERE c.customer_id = s.id
            ORDER BY c.balance DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions c
            WHERE c.customer_id = s.id
              AND c.country IN ('UK', 'JP')
        )
),
pivoted AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            status,
            account_account_type,
            prev_credit_limit_2_whole
        FROM kept
    ) src
    PIVOT (
        MAX(prev_credit_limit_2_whole)
        FOR account_account_type IN ('LOAN' AS loan, 'CREDIT' AS credit)
    )
)
SELECT status, credit
FROM pivoted
ORDER BY status DESC NULLS LAST;
