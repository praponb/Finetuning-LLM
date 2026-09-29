WITH valid_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        start_date,
        end_date,
        status
    FROM policies
    WHERE (status NOT IN ('ACTIVE', 'CANCELLED') OR end_date BETWEEN DATE '2024-11-08' AND DATE '2027-01-03')
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        start_date,
        end_date,
        status,
        DATE_TRUNC('QUARTER', end_date) AS end_date_quarter,
        SUBSTRING(TRIM(status), 1, 3) AS status_prefix
    FROM valid_policys
    WHERE holder_id <= 15
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        DATE_TRUNC('QUARTER', end_date) AS end_date_quarter_2,
        LOWER(TRIM(status_prefix)) AS status_prefix_clean
    FROM derived
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.id) AS claim_count,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.id) AS claim_count_2
    FROM scored s
)
SELECT DISTINCT claim_count_2, end_date_quarter, status_prefix_clean
FROM summarized_rows
ORDER BY status_prefix_clean
