WITH prepared_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        status,
        LENGTH(TRIM(status)) AS status_len,
        FLOOR(premium / 1000) * 1000 AS premium_bucket,
        DATE_TRUNC('YEAR', start_date) AS start_date_year
    FROM policies
    WHERE holder_id >= 19
      AND status = 'CANCELLED'
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM prepared_policys
    WHERE premium_bucket > (SELECT MIN(premium_bucket) FROM prepared_policys)
),
filtered AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM with_activity s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
        )
      AND s.premium > 2111.3
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    premium,
    status_len,
    premium_bucket,
    start_date_year,
    DATE_TRUNC('YEAR', start_date_year) AS start_date_year_year,
    ABS(status_len - 7) AS status_len_distance
FROM filtered
WHERE status = 'ACTIVE'
ORDER BY premium_bucket DESC NULLS FIRST, status_len DESC
