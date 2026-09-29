WITH base AS (
    -- Keep recent, valid rows
    SELECT *, GREATEST(credit_limit, age) AS greatest_credit_limit_age
    FROM customers
    WHERE signup_date >= DATE '2023-09-05'
      AND segment IS NOT DISTINCT FROM 'SMB'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        email,
        country,
        segment,
        signup_date,
        is_active,
        age,
        CUME_DIST() OVER (PARTITION BY country ORDER BY age NULLS LAST) AS age_cume_dist,
        ROUND(age / SUM(age) OVER (PARTITION BY city), 4) AS age_share,
        NTILE(4) OVER (PARTITION BY country ORDER BY credit_limit, id) AS credit_limit_bucket
    FROM base
    WHERE city = 'Singapore'
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY age_share NULLS LAST, id) AS rank_in_group
    FROM with_windows
    QUALIFY rank_in_group = 1
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM accounts ch
            WHERE ch.customer_id = s.id
              AND ch.opened_date <= s.signup_date
        )
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    country,
    metric_name,
    val
FROM (
    SELECT
        id,
        country,
        age_share,
        age_cume_dist
    FROM not_flagged
) src
UNPIVOT (
    val FOR metric_name IN (age_share AS AGE_SHARE, age_cume_dist AS AGE_CUME_DIST)
) long_rows;
