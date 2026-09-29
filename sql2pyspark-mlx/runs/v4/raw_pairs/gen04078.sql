WITH valid AS (
    SELECT
        id,
        customer_id,
        start_date,
        status,
        DATE_ADD(start_date, 5) AS start_date_plus_5d
    FROM subscriptions
    WHERE customer_id IS NOT NULL
      AND status != 'CANCELLED'
)
SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id, status ORDER BY start_date NULLS FIRST, id) AS rank_in_group
FROM valid
QUALIFY rank_in_group = 1;
