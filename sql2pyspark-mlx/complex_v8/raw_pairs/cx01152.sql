WITH scoped_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status,
        UPPER(TRIM(status)) AS status_clean,
        LAST_DAY(end_date) AS end_date_month_end
    FROM policies
    WHERE YEAR(end_date) = 2026
      AND status = 'ACTIVE'
),
with_activity AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM scoped_policys s
    WHERE EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.end_date BETWEEN DATE '2025-12-05' AND DATE '2027-05-16'
              AND p.premium >= 1132.1
        )
),
profiled AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT MAX(p.end_date) FROM policies p WHERE p.holder_id = s.holder_id) AS last_policy_end_date,
        (SELECT MIN(p.start_date) FROM policies p WHERE p.holder_id = s.holder_id) AS first_policy_start_date
    FROM with_activity s
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAG(end_date_month_end, 2) OVER w_start_date_year_product_by_f AS prev2_end_date_month_end,
        ROUND(AVG(premium) OVER (PARTITION BY YEAR(start_date), product ORDER BY first_policy_start_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS premium_avg_3_prior,
        ROUND(premium - AVG(premium) OVER (PARTITION BY MONTH(start_date)), 2) AS premium_vs_start_date_month_avg,
        LAST_VALUE(premium) OVER (PARTITION BY YEAR(start_date), product ORDER BY first_policy_start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_premium
    FROM profiled
    WINDOW w_start_date_year_product_by_f AS (PARTITION BY YEAR(start_date), product ORDER BY first_policy_start_date, id)
)
SELECT
    id,
    holder_id,
    product,
    premium,
    start_date,
    status_clean,
    last_policy_end_date,
    first_policy_start_date,
    premium_avg_3_prior,
    premium_vs_start_date_month_avg,
    last_premium,
    LAST_DAY(end_date_month_end) AS end_date_month_end_month_end
FROM sequenced
WHERE UPPER(TRIM(status_clean)) LIKE 'C%'
ORDER BY premium, first_policy_start_date NULLS LAST
