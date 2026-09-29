WITH valid AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        COALESCE(premium, 0.0) AS premium_filled
    FROM policies
    WHERE premium < 2945.9
),
latest AS (
    -- Keep the earliest row per product, status
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        premium_filled
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY product, status ORDER BY premium, id) AS rn
        FROM valid
    ) ranked
    WHERE rn = 1
),
without_matches AS (
    -- Compare against the overall min
    SELECT *
    FROM latest
    WHERE premium > (SELECT MIN(premium) FROM latest)
),
detail_rows AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.end_date,
        s.status,
        c.id AS claim_id,
        c.claim_date,
        c.status AS claim_status,
        c.claim_type
    FROM without_matches s
    JOIN claims c ON c.policy_id = s.id
)
SELECT
    id,
    holder_id,
    end_date,
    claim_id,
    claim_date,
    claim_status,
    claim_type,
    CASE WHEN claim_status IN ('REJECTED', 'IN_REVIEW') THEN 'group_a' WHEN claim_status = 'PAID' THEN 'group_b' ELSE 'other' END AS claim_status_group
FROM detail_rows
ORDER BY claim_type, id DESC
