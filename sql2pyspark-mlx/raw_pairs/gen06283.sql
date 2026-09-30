WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LAST_DAY(opened_date) AS opened_date_month_end
    FROM accounts
    WHERE opened_date >= DATE '2023-09-20'
      AND customer_id IN (3, 11, 18)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(balance) OVER (PARTITION BY customer_id ORDER BY opened_date_month_end ASC, id ASC) AS next_balance,
        LAG(balance) OVER (PARTITION BY customer_id ORDER BY opened_date_month_end ASC, id ASC) AS prev_balance,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY opened_date_month_end ASC, id ASC) AS seq_no
    FROM cleaned
),
joined AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.balance,
        e.status,
        e.opened_date_month_end,
        e.next_balance,
        e.seq_no,
        c.country,
        c.age
    FROM scored e
    LEFT JOIN customers c
        ON e.customer_id = c.id
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status, customer_id ORDER BY balance ASC NULLS FIRST, id ASC) AS rank_in_group
        FROM joined
    ) t
    WHERE rank_in_group = 1
),
wide AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            status,
            country,
            seq_no
        FROM ranked
    ) src
    PIVOT (
        MAX(seq_no) AS total
        FOR country IN ('DE', 'UK')
    )
)
SELECT
    status,
    DE,
    CASE WHEN status RLIKE '^(CLOSED|FROZEN)$' THEN 'EXTENDED' WHEN status RLIKE '^(OPEN)$' THEN 'PRIORITY' ELSE 'GROUP_B' END AS status_zone
FROM wide
ORDER BY status ASC NULLS FIRST, DE DESC;
