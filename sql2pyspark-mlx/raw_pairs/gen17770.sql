WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, DATEDIFF(CURRENT_DATE(), end_date) days_since_end_date
    FROM subscriptions
    WHERE customer_id IS NOT NULL
      AND end_date > DATE '2023-06-05'
)
-- Deduplicate with ROW_NUMBER
SELECT
    id,
    customer_id,
    plan,
    end_date,
    status
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY start_date NULLS LAST, id) AS rn
    FROM filtered
) t
WHERE rn = 1;
