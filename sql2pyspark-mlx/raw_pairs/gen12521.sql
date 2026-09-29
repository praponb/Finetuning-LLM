WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        INITCAP(TRIM(plan)) AS plan_title,
        CONCAT_WS('-', plan, status) AS plan_status_key
    FROM subscriptions
    WHERE end_date > DATE '2023-04-15'
      AND customer_id IS NOT NULL
)
SELECT plan
FROM filtered
EXCEPT DISTINCT
SELECT plan
FROM subscriptions
WHERE status != 'EXPIRED'
