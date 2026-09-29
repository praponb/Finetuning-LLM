WITH prepared_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        DATE_FORMAT(reported_at, 'yyyy-MM-dd') AS reported_at_label
    FROM claims
    WHERE HOUR(reported_at) BETWEEN 8 AND 14
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER (ORDER BY approved_amount NULLS LAST), 3) AS approved_amount_cume_dist,
        SIZE(COLLECT_SET(adjuster_id) OVER (PARTITION BY claim_type)) AS distinct_adjuster_id_in_claim_type,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY claim_type), 2) AS approved_amount_vs_claim_type_avg,
        DATEDIFF(claim_date, LAG(claim_date) OVER w_claim_type_by_claim_date) AS days_since_prev_claim_date
    FROM prepared_claims
    WINDOW w_claim_type_by_claim_date AS (PARTITION BY claim_type ORDER BY claim_date DESC, id)
)
SELECT *
FROM (
    SELECT distinct_adjuster_id_in_claim_type, claim_type, approved_amount_vs_claim_type_avg
    FROM sequenced
) src
PIVOT (
    MAX(approved_amount_vs_claim_type_avg) AS max_v, SUM(approved_amount_vs_claim_type_avg) AS sum_v
    FOR claim_type IN ('delay', 'theft', 'fire')
)
