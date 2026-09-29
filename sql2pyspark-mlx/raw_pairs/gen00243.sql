WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM subscriptions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY start_date DESC NULLS LAST, id) = 1
)
SELECT
    id,
    plan,
    start_date,
    end_date,
    monthly_fee,
    CASE WHEN plan RLIKE '^(standard|premium)$' THEN 'EXTENDED' WHEN plan RLIKE '^(basic|enterprise)$' THEN 'GROUP_B' ELSE 'PRIORITY' END AS plan_zone
FROM deduped
ORDER BY id DESC NULLS LAST;
