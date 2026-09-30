WITH recent AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date
    FROM policies
    WHERE premium BETWEEN 1541.8 AND 4417.0
      AND product <> 'TRAVEL'
),
without_matches AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM recent s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
        )
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    premium,
    end_date,
    status,
    days_start_date_to_end_date,
    DATE_TRUNC('MONTH', end_date) AS end_date_month
FROM without_matches
