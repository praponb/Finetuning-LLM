WITH cleaned AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        reported_at,
        CASE WHEN status IN ('PAID', 'IN_REVIEW') THEN 'group_a' WHEN status = 'APPROVED' THEN 'group_b' ELSE 'other' END AS status_group,
        DATEDIFF(DATE '2025-12-31', claim_date) AS days_since_claim_date,
        LENGTH(TRIM(status)) AS status_len
    FROM claims
    WHERE claim_type = 'delay'
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        status_len,
        TO_DATE(reported_at) AS reported_at_day,
        ROUND(SUM(approved_amount), 2) AS total_approved_amount,
        ROUND(SUM(days_since_claim_date) / NULLIF(SUM(status_len), 0), 3) AS days_since_claim_date_to_status_len_ratio,
        COUNT(*) FILTER (WHERE claim_type = 'collision') AS n_claim_type_collision
    FROM cleaned
    WHERE policy_id >= 8
    GROUP BY status_len, TO_DATE(reported_at)
),
scored AS (
    -- Label and score rows
    SELECT *, DATE_ADD(reported_at_day, 7) AS reported_at_day_plus_7d
    FROM totals
),
with_activity AS (
    -- Compare against the overall max
    SELECT *
    FROM scored
    WHERE days_since_claim_date_to_status_len_ratio < (SELECT MAX(days_since_claim_date_to_status_len_ratio) FROM scored)
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN status_len >= 11 THEN 'A' WHEN status_len >= 7 THEN 'B' ELSE 'C' END AS status_len_tier,
        total_approved_amount - status_len AS total_approved_amount_minus_status_len
    FROM with_activity
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM tagged
    WHERE total_approved_amount_minus_status_len > (SELECT AVG(total_approved_amount_minus_status_len) FROM tagged)
)
SELECT *
FROM (
    SELECT reported_at_day_plus_7d, reported_at_day, status_len_tier, total_approved_amount
    FROM without_matches
) src
PIVOT (
    MAX(total_approved_amount) AS max_v, COUNT(total_approved_amount) AS n
    FOR status_len_tier IN ('A', 'C', 'B')
)
ORDER BY reported_at_day_plus_7d, reported_at_day
