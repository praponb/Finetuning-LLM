WITH valid_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date
    FROM policies
    WHERE status IN ('LAPSED', 'CANCELLED')
),
qualified AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM valid_policys s
    WHERE s.id IN (
            SELECT c.policy_id
            FROM claims c
            WHERE MONTH(c.reported_at) IN (1, 2)
              AND c.adjuster_id IS NOT NULL
        )
      AND s.end_date < DATE '2026-06-18'
),
stats AS (
    -- Group-level totals
    SELECT
        holder_id,
        YEAR(start_date) AS start_date_year,
        COUNT(*) AS n_rows,
        PERCENTILE_APPROX(premium, 0.5) AS median_premium,
        MAX(end_date) AS last_end_date,
        MIN(premium) AS min_premium,
        SUM(premium) AS total_premium
    FROM qualified
    GROUP BY 1, 2
),
screened AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM stats s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.end_date < DATE '2026-07-11'
        )
      AND s.start_date_year BETWEEN 2024 AND 2025
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        holder_id,
        start_date_year,
        n_rows,
        median_premium,
        last_end_date,
        min_premium,
        total_premium,
        FIRST_VALUE(n_rows) OVER (PARTITION BY n_rows, holder_id ORDER BY n_rows, holder_id, start_date_year ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_n_rows,
        COUNT(*) OVER w_n_rows_holder_id AS rows_in_n_rows_holder_id,
        LEAD(min_premium, 3, 0) OVER w_n_rows_holder_id_by_n_rows AS next3_min_premium,
        ROUND(median_premium / NULLIF(SUM(median_premium) OVER w_n_rows_holder_id, 0), 4) AS median_premium_share_of_n_rows_holder_id,
        MAX(median_premium) OVER w_n_rows_holder_id AS n_rows_holder_id_max_median_premium,
        NTILE(4) OVER (PARTITION BY n_rows, holder_id ORDER BY n_rows, holder_id, start_date_year) AS n_rows_quartile
    FROM screened
    WINDOW w_n_rows_holder_id AS (PARTITION BY n_rows, holder_id), w_n_rows_holder_id_by_n_rows AS (PARTITION BY n_rows, holder_id ORDER BY n_rows, holder_id, start_date_year)
),
exploded_detail AS (
    -- One row per policy
    SELECT
        s.holder_id,
        s.start_date_year,
        s.n_rows,
        s.median_premium,
        s.last_end_date,
        s.first_n_rows,
        s.rows_in_n_rows_holder_id,
        s.next3_min_premium,
        s.n_rows_holder_id_max_median_premium,
        p.id AS policy_id,
        p.product AS policy_product,
        p.status AS policy_status
    FROM sequenced s
    LEFT JOIN policies p ON p.holder_id = s.holder_id AND p.product = 'HOME'
),
policy_product_stats AS (
    -- Max start_date_year per policy_product
    SELECT policy_product, MAX(start_date_year) AS max_start_date_year
    FROM exploded_detail
    GROUP BY policy_product
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_start_date_year,
        ROUND(s.start_date_year - g.max_start_date_year, 2) AS start_date_year_minus_max
    FROM exploded_detail s
    JOIN policy_product_stats g ON g.policy_product = s.policy_product
    WHERE s.start_date_year = g.max_start_date_year
)
SELECT
    holder_id,
    start_date_year,
    n_rows,
    median_premium,
    last_end_date,
    first_n_rows,
    next3_min_premium,
    n_rows_holder_id_max_median_premium,
    policy_product,
    policy_status,
    max_start_date_year,
    start_date_year_minus_max,
    INITCAP(TRIM(policy_product)) AS policy_product_clean,
    DATE_FORMAT(last_end_date, 'yyyy-MM-dd') AS last_end_date_label
FROM above_average
