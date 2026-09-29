WITH lines AS (
    SELECT
        id,
        customer_id,
        created_at,
        value
    FROM carts
    LATERAL VIEW EXPLODE(coupon_codes) e AS value
),
base AS (
    SELECT
        *,
        LENGTH(value) AS value_length,
        QUARTER(created_at) AS created_at_quarter,
        COALESCE(value, 'N/A') AS value_filled
    FROM lines
    WHERE created_at > DATE '2025-03-01'
),
enriched AS (
    SELECT
        src.*,
        c.country,
        c.name,
        c.segment
    FROM base src
    LEFT JOIN customers c
        ON src.customer_id = c.id
),
with_windows AS (
    SELECT *, RANK() OVER (PARTITION BY value ORDER BY created_at_quarter DESC) AS created_at_quarter_rank
    FROM enriched
)
SELECT
    s.id,
    s.customer_id,
    s.created_at,
    s.value_filled,
    s.country,
    s.name,
    s.segment,
    s.created_at_quarter_rank,
    CASE WHEN segment RLIKE '^(SMB|Consumer)$' THEN 'GROUP_A' WHEN segment RLIKE '^(Corporate)$' THEN 'PRIORITY' ELSE 'EXTENDED' END AS segment_zone,
    (SELECT AVG(x.balance) FROM accounts x WHERE x.customer_id = s.customer_id) AS avg_account_balance
FROM with_windows s;
