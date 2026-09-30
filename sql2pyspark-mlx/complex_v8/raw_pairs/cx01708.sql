WITH recent_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        approved_amount,
        reported_at,
        LENGTH(TRIM(status)) AS status_len
    FROM claims
    WHERE status = 'APPROVED'
      AND claim_date < DATE '2025-10-30'
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.status_len,
        a.level AS adjuster_level,
        a.adjuster_name
    FROM recent_claims s
    JOIN adjusters a ON a.id = s.adjuster_id AND UPPER(TRIM(a.adjuster_name)) LIKE 'G%'
    WHERE UPPER(TRIM(a.adjuster_name)) LIKE 'G%'
),
deduped AS (
    -- Keep the latest row per policy_id
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        status_len,
        adjuster_level,
        adjuster_name
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY policy_id ORDER BY adjuster_level DESC, id DESC) AS rn
        FROM combined_details
    ) ranked
    WHERE rn = 1
),
enriched AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.adjuster_level,
        s.adjuster_name,
        a.adjuster_name AS adjuster_adjuster_name,
        a.team AS adjuster_team,
        a.level AS adjuster_level_2
    FROM deduped s
    JOIN adjusters a ON a.id = s.adjuster_id
    WHERE a.level <= 3
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS claim_count,
        (SELECT MIN(c.reported_at) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS first_claim_reported_at,
        (SELECT MAX(c.reported_at) FROM claims c WHERE c.adjuster_id = s.adjuster_id) AS last_claim_reported_at
    FROM enriched s
),
claim_stats_2 AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        COUNT(CASE WHEN status IN ('APPROVED', 'REJECTED', 'IN_REVIEW') THEN 1 END) AS claim_status_count,
        MIN(claim_date) AS first_claim_date,
        MAX(approved_amount) AS max_claim_approved_amount
    FROM claims
    GROUP BY policy_id
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_status_count, 0) AS claim_status_count,
        c.first_claim_date,
        c.max_claim_approved_amount
    FROM with_stats s
    LEFT JOIN claim_stats_2 c ON c.policy_id = s.policy_id
)
SELECT *
FROM (
    SELECT claim_count, adjuster_team, claimed_amount
    FROM summarized_rows
) src
PIVOT (
    MIN(claimed_amount) AS min_v, COUNT(claimed_amount) AS n
    FOR adjuster_team IN ('Property', 'Motor', 'Travel')
)
ORDER BY claim_count
