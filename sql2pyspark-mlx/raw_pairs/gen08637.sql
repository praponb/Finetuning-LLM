WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER w_ordered AS seq_no,
        FIRST_VALUE(browser, TRUE) OVER w_ordered_2 AS first_known_browser,
        ROW_NUMBER() OVER w_grp AS row_seq,
        FIRST_VALUE(country) IGNORE NULLS OVER (PARTITION BY customer_id, device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_country,
        ROW_NUMBER() OVER w_recent AS row_seq_2
    FROM sessions
    WHERE customer_id IN (23, 38)
    WINDOW w_ordered AS (ORDER BY started_at DESC, id), w_ordered_2 AS (PARTITION BY device, customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY customer_id, device ORDER BY started_at DESC, id), w_recent AS (PARTITION BY MONTH(started_at), customer_id ORDER BY started_at DESC, id)
),
remaining_keys AS (
    -- Set difference on keys
    SELECT device, customer_id
    FROM metrics
    EXCEPT ALL
    SELECT device, customer_id
    FROM sessions
    WHERE country = 'JP'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    INNER JOIN remaining_keys k ON s.device = k.device AND s.customer_id = k.customer_id
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        s.*,
        p.name,
        p.segment,
        p.credit_limit
    FROM kept_rows s
    LEFT JOIN customers p
        ON s.customer_id = p.id
),
with_dims_sessions AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.browser,
        e.started_at,
        e.seq_no,
        e.row_seq,
        e.name,
        e.segment,
        e.credit_limit,
        d.age,
        d.city
    FROM with_dims e
    LEFT JOIN customers d
        ON e.customer_id <=> d.id
)
SELECT
    id,
    customer_id,
    browser,
    seq_no,
    row_seq,
    segment,
    CASE WHEN segment RLIKE '^(Consumer|SMB)$' THEN 'EXTENDED' WHEN segment RLIKE '^(Corporate)$' THEN 'GROUP_A' ELSE 'CORE' END AS segment_zone
FROM with_dims_sessions;
