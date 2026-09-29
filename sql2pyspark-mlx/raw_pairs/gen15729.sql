WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        name,
        city,
        is_active,
        age,
        COALESCE(credit_limit, 0) AS credit_limit_filled,
        COALESCE(segment, 'DIRECT') AS resolved_segment
    FROM customers
    WHERE signup_date > TO_DATE('2023-04-17')
      AND city IS NOT NULL
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        name,
        SUM(credit_limit_filled) OVER (PARTITION BY city ORDER BY credit_limit_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_credit_limit_filled,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY credit_limit_filled, id) AS seq_no,
        AVG(credit_limit_filled) OVER (PARTITION BY city ORDER BY age, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS credit_limit_filled_avg_3
    FROM filtered
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM subscriptions ch
            WHERE ch.customer_id = s.id
              AND ch.monthly_fee >= 70
        )
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(cumulative_credit_limit_filled, TRUE) OVER (ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cumulative_credit_limit_filled,
        ROW_NUMBER() OVER (ORDER BY cumulative_credit_limit_filled DESC, id) AS row_seq,
        NTILE(4) OVER (ORDER BY seq_no DESC, id) AS seq_no_bucket
    FROM kept
),
with_latest AS (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.row_seq,
        s.seq_no_bucket,
        latest.device
    FROM with_windows s
    JOIN LATERAL (
            SELECT c.device
            FROM sessions c
            WHERE c.customer_id = s.id
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM with_latest
    INTERSECT ALL
    SELECT id
    FROM customers
    WHERE NOT is_active
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_latest s
    INNER JOIN kept_keys k ON s.id = k.id
)
SELECT
    seq_no_bucket,
    device,
    CASE WHEN device RLIKE '^(mobile|desktop)$' THEN 'CORE' ELSE 'OTHER_ZONE' END AS device_zone
FROM reconciled;
