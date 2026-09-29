WITH valid AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        COALESCE(product, 'UNKNOWN') AS product_or_unknown,
        DATEDIFF(start_date, end_date) AS days_end_date_to_start_date
    FROM policies
    WHERE status IN ('EXPIRED', 'LAPSED', 'ACTIVE')
      AND YEAR(start_date) = 2025
),
holder_id_stats AS (
    -- Avg days_end_date_to_start_date per holder_id
    SELECT holder_id, AVG(days_end_date_to_start_date) AS avg_days_end_date_to_start_date
    FROM valid
    GROUP BY holder_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_days_end_date_to_start_date,
        ROUND(s.days_end_date_to_start_date - g.avg_days_end_date_to_start_date, 2) AS days_end_date_to_start_date_minus_avg
    FROM valid s
    JOIN holder_id_stats g ON g.holder_id = s.holder_id
    WHERE s.days_end_date_to_start_date > g.avg_days_end_date_to_start_date
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        holder_id,
        premium,
        status,
        product_or_unknown,
        days_end_date_to_start_date,
        avg_days_end_date_to_start_date,
        days_end_date_to_start_date_minus_avg,
        DATE_TRUNC('WEEK', end_date) AS end_date_week
    FROM vs_group
),
summarized_rows AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT MAX(p.end_date) FROM policies p WHERE p.holder_id = s.holder_id) AS last_policy_end_date
    FROM scored s
),
derived AS (
    -- Add calculated columns
    SELECT *, LENGTH(TRIM(product_or_unknown)) AS product_or_unknown_len
    FROM summarized_rows
    WHERE avg_days_end_date_to_start_date <= 140.0
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND(product_or_unknown_len / NULLIF(premium, 0), 3) AS product_or_unknown_len_per_premium
    FROM derived
),
joined AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.status,
        s.product_or_unknown,
        s.days_end_date_to_start_date,
        s.avg_days_end_date_to_start_date,
        s.days_end_date_to_start_date_minus_avg,
        s.end_date_week,
        s.last_policy_end_date,
        s.product_or_unknown_len_per_premium,
        p.risk_score AS holder_risk_score
    FROM tagged s
    JOIN policyholders p ON p.id = s.holder_id
)
-- Final projection
SELECT
    id,
    holder_id,
    premium,
    product_or_unknown,
    days_end_date_to_start_date,
    avg_days_end_date_to_start_date,
    days_end_date_to_start_date_minus_avg,
    end_date_week,
    product_or_unknown_len_per_premium,
    holder_risk_score
FROM joined
ORDER BY product_or_unknown DESC
