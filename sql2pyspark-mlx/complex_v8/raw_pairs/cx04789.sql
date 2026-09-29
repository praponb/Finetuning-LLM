WITH prepared AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE product NOT IN ('AUTO', 'HOME')
),
filtered AS (
    -- Keep rows whose holder matches
    SELECT s.*
    FROM prepared s
    WHERE EXISTS (
            SELECT 1
            FROM policyholders p
            WHERE p.id = s.holder_id
              AND p.segment = 'SME'
        )
),
with_details_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        p.joined_on AS holder_joined_on,
        p.risk_score AS holder_risk_score
    FROM filtered s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.joined_on < DATE '2025-02-22'
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(premium - AVG(premium) OVER (PARTITION BY MONTH(start_date), MONTH(holder_joined_on)), 2) AS premium_vs_start_date_month_holder_joined_on_mon,
        FIRST_VALUE(holder_joined_on) OVER (PARTITION BY holder_id, product ORDER BY holder_risk_score ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_holder_joined_on,
        ROUND(holder_risk_score / NULLIF(SUM(holder_risk_score) OVER (PARTITION BY MONTH(start_date), MONTH(holder_joined_on)), 0), 4) AS holder_risk_score_share_of_start_date_month_hold,
        LAG(holder_joined_on) OVER w_holder_id_product_by_holder_2 AS prev_holder_joined_on,
        SUM(premium) OVER w_start_date_month_holder_join AS start_date_month_holder_joined_on_month_sum_prem
    FROM with_details_holder
    WINDOW w_holder_id_product_by_holder_2 AS (PARTITION BY holder_id, product ORDER BY holder_risk_score ASC NULLS LAST, id), w_start_date_month_holder_join AS (PARTITION BY MONTH(start_date), MONTH(holder_joined_on))
)
-- Subtotals with CUBE
SELECT
    COALESCE(product, 'ALL') AS product,
    holder_id,
    MIN(premium_vs_start_date_month_holder_joined_on_mon) AS min_premium_vs_start_date_month_holder_joined_on
FROM sequenced
GROUP BY CUBE(product, holder_id)
