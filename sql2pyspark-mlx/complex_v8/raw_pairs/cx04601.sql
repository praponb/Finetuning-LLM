WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        CONCAT_WS(': ', status, claim_type) AS status_claim_type_label
    FROM claims
    WHERE adjuster_id IS NOT NULL
      AND YEAR(claim_date) = 2024
),
joined_adjuster AS (
    -- Join adjusters
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.status,
        s.status_claim_type_label,
        a.team AS adjuster_team
    FROM prepared s
    LEFT JOIN adjusters a ON a.id = s.adjuster_id
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        status,
        status_claim_type_label,
        ROUND(CUME_DIST() OVER (ORDER BY claim_date NULLS LAST, id), 3) AS claim_date_cume_dist,
        DENSE_RANK() OVER (PARTITION BY policy_id ORDER BY claim_date DESC) AS claim_date_dense_rank,
        NTH_VALUE(claim_date, 2) OVER (PARTITION BY policy_id ORDER BY claim_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claim_date,
        ROUND(PERCENT_RANK() OVER (ORDER BY claim_date NULLS LAST), 3) AS claim_date_percent_rank
    FROM joined_adjuster
)
SELECT
    claim_date_dense_rank,
    claim_type,
    GROUPING(claim_date_dense_rank) AS is_total_claim_date_dense_rank,
    MAX(claim_date_cume_dist) AS max_claim_date_cume_dist,
    ARRAY_SORT(COLLECT_SET(claim_type)) AS claim_type_list,
    ARRAY_SORT(COLLECT_SET(status)) AS status_list
FROM windowed
GROUP BY GROUPING SETS ((claim_date_dense_rank, claim_type), (claim_date_dense_rank), (claim_type), ())
ORDER BY claim_date_dense_rank NULLS LAST, claim_type NULLS LAST
