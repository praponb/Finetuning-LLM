WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, DATE_ADD(opened_date, 19) AS opened_date_plus_19d
    FROM accounts
    WHERE customer_id IS NOT NULL
),
enriched AS (
    -- Attach customers attributes
    SELECT
        s.*,
        p.credit_limit,
        p.segment,
        p.signup_date
    FROM valid s
    INNER JOIN customers p
        ON s.customer_id = p.id
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.is_flagged = TRUE
        )
),
joined AS (
    -- Attach customers attributes
    SELECT
        src.*,
        d.country,
        d.segment AS customer_segment
    FROM kept src
    LEFT JOIN customers d
        ON src.customer_id = d.id
),
kept_keys AS (
    -- Set intersection on keys
    SELECT customer_id, status
    FROM joined
    INTERSECT ALL
    SELECT customer_id, status
    FROM accounts
    WHERE balance < 28000
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM joined s
    INNER JOIN kept_keys k ON s.customer_id = k.customer_id AND s.status = k.status
)
-- Join a dimension and aggregate
SELECT
    p.segment AS customer_segment,
    src.customer_segment AS account_customer_segment,
    COUNT(*) AS rows_total,
    MIN(src.credit_limit) AS min_credit_limit,
    GROUPING_ID() AS gid
FROM kept_rows src
LEFT JOIN customers p ON src.customer_id = p.id
GROUP BY GROUPING SETS ((p.segment, src.customer_segment), (p.segment))
