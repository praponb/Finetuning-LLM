WITH filtered_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        premium,
        end_date,
        status,
        DATE_TRUNC('MONTH', start_date) AS start_date_month,
        LOWER(REGEXP_REPLACE(TRIM(product), ' ', '_')) AS product_slug,
        COALESCE(premium, 0.0) AS premium_filled
    FROM policies
    WHERE YEAR(end_date) = 2026
      AND premium BETWEEN 1042.8 AND 3719.7
),
screened AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM filtered_policys s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.claimed_amount < 33975.8
              AND c.claim_type <> 'fire'
        )
),
with_activity AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM screened s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.approved_amount < 20843.1
              AND c.claimed_amount > 12726.7
        )
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTILE(4) OVER (ORDER BY start_date_month DESC, id) AS start_date_month_quartile,
        COUNT(*) OVER (PARTITION BY TO_DATE(start_date_month)) AS rows_in_start_date_month_to_date
    FROM with_activity
)
SELECT *
FROM (
    SELECT end_date, status, premium_filled
    FROM windowed
) src
PIVOT (
    MAX(premium_filled)
    FOR status IN ('CANCELLED', 'LAPSED')
)
ORDER BY end_date
