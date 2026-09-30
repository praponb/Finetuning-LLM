WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN browser IN ('Edge', 'Safari') THEN 'Priority' ELSE 'Other' END AS browser_group,
        DAYOFWEEK(started_at) AS started_at_dayofweek
    FROM sessions
    WHERE customer_id IN (12, 26, 36)
),
with_dims AS (
    -- Attach customers attributes
    SELECT s.*, b.signup_date
    FROM cleaned s
    INNER JOIN customers b
        ON s.customer_id = b.id
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_dims s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.event_type IN ('purchase', 'page_view')
        )
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        device,
        country,
        started_at_dayofweek
    FROM not_flagged
) src
PIVOT (
    MAX(started_at_dayofweek) AS total,
    COUNT(1) AS cnt
    FOR country IN ('US', 'UK')
)
