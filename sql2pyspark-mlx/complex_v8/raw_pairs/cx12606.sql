WITH prepared_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status,
        CONCAT_WS(' / ', status, product) AS status_product_label
    FROM policies
    WHERE end_date IS NOT NULL
),
qualified AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM prepared_policys s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.product IS NOT NULL
              AND p.start_date >= DATE '2024-06-19'
        )
      AND YEAR(s.end_date) = 2026
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY start_date ASC, id) AS seq_no,
        ROUND(PERCENT_RANK() OVER w_all_by_start_date_ties, 3) AS start_date_percent_rank,
        DATEDIFF(end_date, LAG(end_date) OVER (ORDER BY start_date ASC, id)) AS days_since_prev_end_date,
        LAG(start_date, 2) OVER (ORDER BY start_date ASC, id) AS prev2_start_date,
        NTILE(4) OVER (ORDER BY start_date ASC, id) AS start_date_quartile,
        ROUND(CUME_DIST() OVER (ORDER BY start_date ASC, id), 3) AS start_date_cume_dist
    FROM qualified
    WINDOW w_all_by_start_date_ties AS (ORDER BY start_date ASC)
),
summary AS (
    -- Summarise per group
    SELECT
        seq_no,
        DATE_TRUNC('MONTH', prev2_start_date) AS prev2_start_date_month,
        status,
        MIN(start_date) AS first_start_date,
        SUM(start_date_quartile) AS total_start_date_quartile
    FROM metrics
    GROUP BY seq_no, DATE_TRUNC('MONTH', prev2_start_date), status
),
without_matches AS (
    -- Compare against the overall min
    SELECT *
    FROM summary
    WHERE seq_no > (SELECT MIN(seq_no) FROM summary)
)
SELECT
    seq_no,
    prev2_start_date_month,
    status,
    first_start_date,
    total_start_date_quartile,
    ADD_MONTHS(first_start_date, 6) AS first_start_date_plus_6m
FROM without_matches
ORDER BY first_start_date DESC, status
