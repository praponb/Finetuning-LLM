WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN balance >= 88000 THEN 'High' WHEN balance >= 21000 THEN 'Medium' ELSE 'Low' END AS balance_level,
        CASE WHEN status = 'FROZEN' THEN 1 ELSE 0 END AS is_frozen
    FROM accounts
    WHERE opened_date >= DATE '2023-06-15'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        balance_level,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY balance, id) AS seq_no,
        COUNT(is_frozen) OVER (PARTITION BY MONTH(opened_date), customer_id) AS opened_date_month_count_is_frozen
    FROM cleaned
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        GREATEST(seq_no, opened_date_month_count_is_frozen) AS greatest_seq_no_opened_date_month_count_is_frozen,
        COALESCE(seq_no, 0) AS seq_no_filled
    FROM with_windows
    WHERE customer_id IS NOT NULL
      AND seq_no <= 4
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.account_id IN (5, 31, 36)
        )
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        seq_no,
        opened_date_month_count_is_frozen,
        greatest_seq_no_opened_date_month_count_is_frozen,
        STDDEV_SAMP(opened_date_month_count_is_frozen) OVER (PARTITION BY customer_id) AS customer_id_stddev_samp_opened_date_month_count_is_frozen,
        SUM(greatest_seq_no_opened_date_month_count_is_frozen) OVER (PARTITION BY customer_id ORDER BY seq_no_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_greatest_seq_no_opened_date_month_count_is_frozen
    FROM eligible
    WHERE seq_no < 8
),
enriched AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.opened_date_month_count_is_frozen,
        e.customer_id_stddev_samp_opened_date_month_count_is_frozen,
        e.cumulative_greatest_seq_no_opened_date_month_count_is_frozen,
        d.segment
    FROM scored e
    LEFT JOIN customers d
        ON e.customer_id = d.id
),
enriched_accounts AS (
    -- Attach customers attributes
    SELECT a.*, b.country
    FROM enriched a
    LEFT JOIN customers b
        ON a.customer_id = b.id
)
-- Join a dimension and aggregate
SELECT
    c.country AS customer_country,
    a.segment AS account_segment,
    COUNT(*) AS rows_total,
    MIN(a.cumulative_greatest_seq_no_opened_date_month_count_is_frozen) AS min_cumulative_greatest_seq_no_opened_date_month_count_is_frozen,
    GROUPING_ID() AS level_id
FROM enriched_accounts a
LEFT JOIN customers c ON a.customer_id = c.id
GROUP BY GROUPING SETS ((c.country, a.segment), (c.country), ())
ORDER BY rows_total NULLS FIRST
