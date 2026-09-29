WITH base AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        COALESCE(claim_type, 'UNKNOWN') AS claim_type_or_unknown
    FROM claims
    WHERE (claim_type <> 'collision' OR policy_id IS NOT NULL)
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(claim_type) OVER w_adjuster_id_claim_type_or_un) AS distinct_claim_type_in_adjuster_id_claim_type_or,
        ROW_NUMBER() OVER (PARTITION BY claim_type ORDER BY reported_at DESC, id) AS reported_at_seq,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY claim_type_or_unknown, adjuster_id ORDER BY claimed_amount NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS claimed_amount_avg_4,
        ROUND(PERCENT_RANK() OVER (PARTITION BY claim_type ORDER BY reported_at DESC), 3) AS reported_at_percent_rank
    FROM base
    WINDOW w_adjuster_id_claim_type_or_un AS (PARTITION BY adjuster_id, claim_type_or_unknown)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, value
FROM (
    SELECT id, claimed_amount, reported_at_percent_rank, claimed_amount_avg_4
    FROM windowed
) src
UNPIVOT (
    value FOR metric_name IN (claimed_amount, reported_at_percent_rank, claimed_amount_avg_4)
) unp
ORDER BY id, metric_name
