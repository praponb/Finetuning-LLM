WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        status,
        reported_at,
        LENGTH(TRIM(status)) AS status_len
    FROM claims
    WHERE adjuster_id IS NOT NULL
      AND policy_id >= 13
),
deduped AS (
    -- Keep the latest row per status
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        status,
        reported_at,
        status_len
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY status ORDER BY reported_at DESC, id DESC) AS recency_rank
        FROM cleaned
    ) ranked
    WHERE recency_rank = 1
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(status_len - AVG(status_len) OVER (PARTITION BY policy_id), 2) AS status_len_vs_policy_id_avg,
        COUNT(*) OVER (PARTITION BY policy_id) AS rows_in_policy_id,
        NTH_VALUE(claim_date, 2) OVER (PARTITION BY policy_id, status_len ORDER BY claimed_amount ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claim_date,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY policy_id), 2) AS claimed_amount_vs_policy_id_avg,
        LEAD(claim_date) OVER (PARTITION BY policy_id, status_len ORDER BY claimed_amount ASC, id) AS next_claim_date
    FROM deduped
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.status,
        s.reported_at,
        s.status_len,
        s.status_len_vs_policy_id_avg,
        s.rows_in_policy_id,
        s.claimed_amount_vs_policy_id_avg,
        s.next_claim_date,
        a.level AS adjuster_level,
        p.premium AS policy_premium,
        p.end_date AS policy_end_date
    FROM with_windows s
    JOIN adjusters a ON a.id = s.adjuster_id AND a.level BETWEEN 2 AND 3
    LEFT JOIN policies p ON p.id = s.policy_id
)
SELECT
    policy_id,
    adjuster_id,
    adjuster_level,
    GROUPING_ID() AS gid,
    MIN(rows_in_policy_id) AS min_rows_in_policy_id,
    MIN(status_len_vs_policy_id_avg) AS min_status_len_vs_policy_id_avg
FROM with_details
GROUP BY GROUPING SETS ((policy_id, adjuster_id, adjuster_level), (policy_id))
ORDER BY policy_id NULLS LAST, adjuster_id NULLS LAST, adjuster_level NULLS LAST
