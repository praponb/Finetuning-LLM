WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, DATE_ADD(start_date, 82) AS start_date_plus_82d
    FROM subscriptions
    WHERE start_date > DATE '2023-08-24'
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        e.*,
        d.name,
        d.country,
        d.segment
    FROM cleaned e
    LEFT JOIN customers d
        ON e.customer_id = d.id
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        start_date_plus_82d,
        name,
        country,
        CASE WHEN plan IN ('standard', 'premium') THEN 'Group A' ELSE 'Other' END AS plan_group,
        ROUND(monthly_fee * 0.9, 2) AS monthly_fee_scaled
    FROM with_dims
    WHERE segment IS NOT NULL
)
SELECT
    id,
    customer_id,
    plan,
    start_date,
    end_date,
    monthly_fee,
    start_date_plus_82d,
    country,
    plan_group,
    CASE WHEN plan RLIKE '^(premium|enterprise)$' THEN 'GROUP_A' ELSE 'OTHER_ZONE' END AS plan_zone
FROM valid
ORDER BY id DESC, start_date_plus_82d;
