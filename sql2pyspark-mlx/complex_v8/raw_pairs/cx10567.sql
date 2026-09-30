WITH cleaned_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        ROUND(premium * 1.5, 2) AS premium_adj,
        LENGTH(TRIM(status)) AS status_len,
        YEAR(end_date) AS end_date_year
    FROM policies
    WHERE product IS NOT NULL
      AND end_date < DATE '2026-09-19'
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        COUNT(*) AS claim_count,
        COUNT(*) FILTER (WHERE status = 'IN_REVIEW') AS claim_status_count,
        COUNT(*) AS claim_count_2
    FROM claims
    GROUP BY policy_id
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_count, 0) AS claim_count,
        COALESCE(c.claim_status_count, 0) AS claim_status_count,
        COALESCE(c.claim_count_2, 0) AS claim_count_2
    FROM cleaned_policys s
    LEFT JOIN claim_stats c ON c.policy_id = s.id
),
enriched AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.end_date,
        s.status,
        s.status_len,
        s.claim_count,
        s.claim_status_count,
        s.claim_count_2,
        p.risk_score AS holder_risk_score,
        p.joined_on AS holder_joined_on
    FROM with_counts s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM enriched
    WHERE claim_count_2 > (SELECT AVG(claim_count_2) FROM enriched)
)
SELECT
    claim_count,
    COALESCE(status, 'ALL') AS status,
    COALESCE(product, 'ALL') AS product,
    GROUPING_ID() AS gid,
    MAX(claim_status_count) AS max_claim_status_count,
    ROUND(STDDEV(status_len), 2) AS stddev_status_len
FROM without_matches
GROUP BY ROLLUP(claim_count, status, product)
ORDER BY claim_count NULLS LAST, status NULLS LAST, product NULLS LAST
