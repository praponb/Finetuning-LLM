WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE YEAR(end_date) = 2026
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(premium) OVER w_product_end_date_month AS product_end_date_month_max_premium,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY start_date ASC, id) AS seq_no
    FROM prepared
    WINDOW w_product_end_date_month AS (PARTITION BY product, MONTH(end_date))
),
with_parent_holder AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.start_date,
        s.status,
        s.seq_no,
        p.risk_score AS holder_risk_score,
        p.joined_on AS holder_joined_on
    FROM windowed s
    JOIN policyholders p ON p.id = s.holder_id
),
filtered AS (
    -- Keep rows whose holder matches
    SELECT s.*
    FROM with_parent_holder s
    WHERE EXISTS (
            SELECT 1
            FROM policyholders p
            WHERE p.id = s.holder_id
              AND UPPER(TRIM(p.holder_name)) LIKE 'G%'
        )
),
filtered_2 AS (
    -- Compare against the overall avg
    SELECT *
    FROM filtered
    WHERE premium > (SELECT AVG(premium) FROM filtered)
),
combined_details AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.start_date,
        s.status,
        s.seq_no,
        s.holder_joined_on,
        p.joined_on AS holder_joined_on_2
    FROM filtered_2 s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.joined_on < DATE '2025-04-20'
),
screened AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM combined_details s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
        )
      AND s.holder_id IS NOT NULL
)
SELECT *
FROM (
    SELECT holder_id, start_date, status, seq_no
    FROM screened
) src
PIVOT (
    MAX(seq_no) AS max_v, COUNT(seq_no) AS n
    FOR status IN ('EXPIRED', 'LAPSED', 'ACTIVE')
)
ORDER BY holder_id, start_date
