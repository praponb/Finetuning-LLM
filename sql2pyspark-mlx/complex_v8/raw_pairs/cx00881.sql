WITH valid AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        status,
        ABS(premium - 2844.1) AS premium_distance
    FROM policies
    WHERE YEAR(end_date) = 2026
),
with_parent_holder AS (
    -- Join policyholders
    SELECT s.*, p.joined_on AS holder_joined_on, p.risk_score AS holder_risk_score
    FROM valid s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        DATEDIFF(start_date, holder_joined_on) AS days_holder_joined_on_to_start_date,
        UPPER(TRIM(status)) AS status_clean
    FROM with_parent_holder
)
SELECT
    id,
    holder_id,
    premium,
    start_date,
    status,
    premium_distance,
    holder_joined_on,
    holder_risk_score,
    status_clean,
    INITCAP(TRIM(status)) AS status_clean_2,
    UPPER(TRIM(status)) AS status_clean_3
FROM derived
ORDER BY status_clean_3 NULLS LAST, id
LIMIT 5
