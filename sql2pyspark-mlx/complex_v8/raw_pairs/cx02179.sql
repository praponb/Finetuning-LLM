WITH base_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        DATEDIFF(start_date, end_date) AS days_end_date_to_start_date,
        YEAR(end_date) AS end_date_year
    FROM policies
    WHERE (premium BETWEEN 1042.5 AND 4906.3 OR start_date IS NOT NULL)
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN product IN ('AUTO', 'TRAVEL') THEN 'group_a' WHEN product = 'LIFE' THEN 'group_b' ELSE 'other' END AS product_group,
        premium + end_date_year AS premium_plus_end_date_year,
        DATE_TRUNC('QUARTER', end_date) AS end_date_quarter
    FROM base_policys
    WHERE status = 'LAPSED'
),
joined_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.end_date,
        s.status,
        s.end_date_year,
        s.product_group,
        p.risk_score AS holder_risk_score
    FROM annotated s
    JOIN policyholders p ON p.id = s.holder_id
),
latest AS (
    -- Keep the latest row per product_group, holder_id
    SELECT
        id,
        holder_id,
        premium,
        end_date,
        status,
        end_date_year,
        product_group,
        holder_risk_score
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY product_group, holder_id ORDER BY end_date DESC, id DESC) AS recency_rank
        FROM joined_holder
    ) ranked
    WHERE recency_rank = 1
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        holder_id,
        premium,
        end_date,
        end_date_year,
        product_group,
        ROUND(CUME_DIST() OVER (PARTITION BY holder_id ORDER BY end_date NULLS LAST), 3) AS end_date_cume_dist,
        ROUND(end_date_year - AVG(end_date_year) OVER (PARTITION BY status, holder_id), 2) AS end_date_year_vs_status_holder_id_avg
    FROM latest
)
SELECT
    id,
    holder_id,
    end_date,
    end_date_year,
    product_group,
    end_date_cume_dist,
    end_date_year_vs_status_holder_id_avg,
    CONCAT(product_group, '#', CAST(holder_id AS STRING)) AS product_group_ref
FROM with_windows
