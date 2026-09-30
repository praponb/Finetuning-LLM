WITH valid_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        DATE_TRUNC('QUARTER', start_date) AS start_date_quarter,
        COALESCE(premium, 0.0) AS premium_filled
    FROM policies
    WHERE product IN ('LIFE', 'TRAVEL', 'AUTO')
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        start_date_quarter,
        premium_filled,
        ROUND(premium_filled / NULLIF(SUM(premium_filled) OVER w_holder_id_start_date_quarter, 0), 4) AS premium_filled_share_of_holder_id_start_date_qua,
        ROUND(premium_filled - AVG(premium_filled) OVER (PARTITION BY holder_id, MONTH(start_date_quarter)), 2) AS premium_filled_vs_holder_id_start_date_quarter_m,
        FIRST_VALUE(end_date) OVER (PARTITION BY YEAR(start_date) ORDER BY end_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_end_date,
        COUNT(*) OVER (PARTITION BY holder_id, MONTH(start_date_quarter)) AS rows_in_holder_id_start_date_quarter_month
    FROM valid_policys
    WINDOW w_holder_id_start_date_quarter AS (PARTITION BY holder_id, MONTH(start_date_quarter))
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        holder_id,
        premium,
        end_date,
        premium_filled,
        premium_filled_vs_holder_id_start_date_quarter_m,
        first_end_date,
        ADD_MONTHS(end_date, 1) AS end_date_plus_1m
    FROM metrics
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        holder_id,
        premium_filled_vs_holder_id_start_date_quarter_m,
        first_end_date,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date_plus_1m)) AS rows_in_end_date_plus_1m_month,
        ROUND(AVG(premium_filled) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date_plus_1m)), 2) AS end_date_plus_1m_month_avg_premium_filled,
        DATEDIFF(end_date_plus_1m, LAG(end_date_plus_1m) OVER w_end_date_year_by_premium_fil) AS days_since_prev_end_date_plus_1m,
        premium - LAG(premium) OVER (PARTITION BY YEAR(end_date) ORDER BY premium_filled_vs_holder_id_start_date_quarter_m, id) AS premium_change,
        DENSE_RANK() OVER (PARTITION BY YEAR(end_date) ORDER BY premium_filled_vs_holder_id_start_date_quarter_m, id) AS premium_filled_vs_holder_id_start_date_quarter_m_2
    FROM tagged
    WINDOW w_end_date_year_by_premium_fil AS (PARTITION BY YEAR(end_date) ORDER BY premium_filled_vs_holder_id_start_date_quarter_m, id)
),
unique_rows AS (
    -- Keep the earliest row per holder_id
    SELECT
        id,
        holder_id,
        premium_filled_vs_holder_id_start_date_quarter_m,
        first_end_date,
        rows_in_end_date_plus_1m_month,
        end_date_plus_1m_month_avg_premium_filled,
        days_since_prev_end_date_plus_1m,
        premium_change,
        premium_filled_vs_holder_id_start_date_quarter_m_2
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY holder_id ORDER BY end_date_plus_1m_month_avg_premium_filled, id) AS row_num
        FROM with_trends
    ) ranked
    WHERE row_num = 1
),
holder_id_stats AS (
    -- Median premium_change per holder_id
    SELECT holder_id, PERCENTILE_APPROX(premium_change, 0.5) AS median_premium_change
    FROM unique_rows
    GROUP BY holder_id
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_premium_change,
        ROUND(s.premium_change - g.median_premium_change, 2) AS premium_change_minus_median
    FROM unique_rows s
    JOIN holder_id_stats g ON g.holder_id = s.holder_id
    WHERE s.premium_change >= g.median_premium_change
),
with_details AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.premium_filled_vs_holder_id_start_date_quarter_m,
        s.first_end_date,
        s.rows_in_end_date_plus_1m_month,
        s.end_date_plus_1m_month_avg_premium_filled,
        s.premium_change,
        s.premium_filled_vs_holder_id_start_date_quarter_m_2,
        s.premium_change_minus_median,
        p.region AS holder_region
    FROM compared s
    JOIN policyholders p ON p.id = s.holder_id
)
-- Final projection
SELECT DISTINCT id, first_end_date, premium_change_minus_median
FROM with_details
ORDER BY premium_change_minus_median
