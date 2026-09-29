WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        start_date,
        monthly_fee,
        status,
        ROUND(monthly_fee, 2) AS monthly_fee_rounded
    FROM subscriptions
    WHERE customer_id IS NOT NULL
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(monthly_fee, 2, 0) OVER (PARTITION BY customer_id ORDER BY start_date, id) AS next_monthly_fee,
        ROW_NUMBER() OVER w_part AS row_seq,
        LAG(monthly_fee, 2) OVER (PARTITION BY customer_id, status ORDER BY monthly_fee_rounded, id) AS prev_monthly_fee
    FROM valid
    WINDOW w_part AS (ORDER BY start_date DESC, id)
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM with_windows s
    JOIN subscriptions sub
        ON s.customer_id = sub.customer_id
       AND s.start_date BETWEEN sub.start_date AND sub.end_date
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        e.*,
        b.country,
        b.name
    FROM matched e
    LEFT JOIN customers b
        ON e.customer_id = b.id
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        customer_id,
        plan,
        monthly_fee
    FROM with_dims
) src
PIVOT (
    MAX(monthly_fee)
    FOR plan IN ('basic' AS basic, 'enterprise' AS enterprise)
)
