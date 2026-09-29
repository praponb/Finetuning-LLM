WITH valid_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        DATE_FORMAT(start_date, 'yyyy-MM') AS start_date_label
    FROM policies
),
joined_holder AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.end_date,
        s.status,
        s.start_date_label,
        p.segment AS holder_segment,
        p.joined_on AS holder_joined_on
    FROM valid_policys s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE YEAR(p.joined_on) = 2025
),
joined_holder_2 AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.status,
        s.start_date_label,
        s.holder_joined_on,
        p.holder_name,
        p.joined_on AS holder_joined_on_2,
        p.risk_score AS holder_risk_score
    FROM joined_holder s
    JOIN policyholders p ON p.id = s.holder_id AND UPPER(TRIM(p.holder_name)) LIKE 'L%'
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_id,
        product,
        premium,
        status,
        start_date_label,
        holder_joined_on,
        holder_name,
        holder_joined_on_2,
        holder_risk_score,
        NTILE(3) OVER w_holder_joined_on_2_month_hol AS holder_joined_on_2_tercile,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', holder_joined_on_2), holder_id ORDER BY holder_joined_on_2 ASC) AS holder_joined_on_2_dense_rank,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY MONTH(holder_joined_on)), 0), 4) AS premium_share_of_holder_joined_on_month,
        MIN(premium) OVER (PARTITION BY MONTH(holder_joined_on)) AS holder_joined_on_month_min_premium
    FROM joined_holder_2
    WINDOW w_holder_joined_on_2_month_hol AS (PARTITION BY DATE_TRUNC('MONTH', holder_joined_on_2), holder_id ORDER BY holder_joined_on_2 ASC, id), w_holder_joined_on_month AS (PARTITION BY MONTH(holder_joined_on))
)
SELECT DISTINCT product
FROM sequenced
