WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATE_SUB(opened_date, 57) AS opened_date_minus_57d,
        CASE WHEN status IN ('FROZEN', 'OPEN') THEN 'Core' ELSE 'Other' END AS status_group
    FROM accounts
    WHERE opened_date >= DATE '2023-09-16'
      AND status IS NOT NULL
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(balance) OVER (PARTITION BY account_type, status ORDER BY balance ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS balance_avg_6_prior,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY opened_date ASC, id ASC) AS event_seq,
        NTILE(4) OVER (PARTITION BY account_type ORDER BY balance ASC, id ASC) AS balance_bucket,
        LEAD(balance, 1) OVER (PARTITION BY account_type ORDER BY opened_date ASC, id ASC) AS next_balance
    FROM valid
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        src.id,
        src.customer_id,
        src.opened_date_minus_57d,
        src.status_group,
        src.balance_avg_6_prior,
        src.event_seq,
        src.next_balance,
        c.segment
    FROM metrics src
    LEFT JOIN customers c
        ON src.customer_id <=> c.id
    WHERE c.is_active = TRUE
),
with_latest AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.merchant
    FROM with_dims s
    LEFT JOIN LATERAL (
            SELECT c.merchant
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY event_seq ASC NULLS FIRST, id ASC) AS latest_rank
        FROM with_latest
    ) ranked
    WHERE latest_rank = 1
)
-- Join a dimension and aggregate
SELECT
    c.segment AS customer_segment,
    a.merchant,
    COUNT(*) AS cnt,
    MIN(a.next_balance) AS min_next_balance
FROM latest a
INNER JOIN customers c ON a.customer_id = c.id
GROUP BY c.segment, a.merchant
