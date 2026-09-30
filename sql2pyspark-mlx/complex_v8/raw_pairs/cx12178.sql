WITH prepared_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status
    FROM policies
    WHERE (status IS NOT NULL OR start_date < DATE '2025-05-30')
),
latest AS (
    -- Keep the latest row per holder_id, status
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY holder_id, status ORDER BY start_date DESC, id DESC) AS rn
        FROM prepared_policys
    ) ranked
    WHERE rn = 1
),
without_matches AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.claim_date IS NOT NULL
        )
      AND s.product = 'TRAVEL'
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    premium,
    status
FROM without_matches
WHERE start_date BETWEEN DATE '2024-10-22' AND DATE '2025-08-24'
ORDER BY product DESC, id
LIMIT 20
