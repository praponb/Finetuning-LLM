WITH base AS (
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
    WHERE end_date IS NOT NULL
),
annotated AS (
    -- Derive helper columns
    SELECT *, MONTH(end_date) AS end_date_month
    FROM base
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        DAYOFWEEK(end_date) AS end_date_dayofweek,
        CASE WHEN product IN ('TRAVEL', 'HEALTH') THEN 'group_a' WHEN product = 'LIFE' THEN 'group_b' ELSE 'other' END AS product_group,
        COALESCE(product, 'UNKNOWN') AS product_or_unknown
    FROM annotated
    WHERE end_date_month BETWEEN 3 AND 11
),
with_details_holder AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.end_date,
        s.end_date_dayofweek,
        p.joined_on AS holder_joined_on
    FROM enriched s
    JOIN policyholders p ON p.id = s.holder_id
)
SELECT *
FROM (
    SELECT holder_joined_on, product, premium
    FROM with_details_holder
) src
PIVOT (
    AVG(premium)
    FOR product IN ('HEALTH', 'HOME', 'LIFE', 'AUTO')
)
