WITH filtered AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE premium > 2013.8
),
filtered_2 AS (
    -- Compare against the overall avg
    SELECT *
    FROM filtered
    WHERE premium > (SELECT AVG(premium) FROM filtered)
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        premium,
        start_date,
        end_date,
        status,
        DATE_FORMAT(start_date, 'MMM yyyy') AS start_date_label
    FROM filtered_2
),
latest_per_key AS (
    -- Keep the latest row per status
    SELECT
        id,
        premium,
        start_date,
        end_date,
        status,
        start_date_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY status ORDER BY premium DESC, id DESC) AS rn
        FROM scored
    ) ranked
    WHERE rn = 1
),
filtered_3 AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM latest_per_key s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
        )
      AND s.start_date_label IS NOT NULL
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MIN(c.reported_at) FROM claims c WHERE c.policy_id = s.id) AS first_claim_reported_at,
        (SELECT COUNT(CASE WHEN c.claim_type <> 'medical' THEN 1 END) FROM claims c WHERE c.policy_id = s.id) AS claim_claim_type_count
    FROM filtered_3 s
)
-- Final projection
SELECT
    id,
    premium,
    end_date,
    status,
    start_date_label,
    first_claim_reported_at,
    claim_claim_type_count
FROM with_stats
