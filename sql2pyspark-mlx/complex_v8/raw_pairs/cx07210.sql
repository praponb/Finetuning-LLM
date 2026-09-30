WITH cleaned AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at,
        FLOOR(claimed_amount / 10000) * 10000 AS claimed_amount_bucket,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug,
        CONCAT(claim_type, '#', CAST(adjuster_id AS STRING)) AS claim_type_ref
    FROM claims
    WHERE claim_type NOT IN ('fire', 'flood')
),
enriched AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.approved_amount,
        s.reported_at,
        s.status_slug,
        p.status AS policy_status,
        po.region AS holder_region
    FROM cleaned s
    JOIN policies p ON p.id = s.policy_id
    JOIN policyholders po ON po.id = p.holder_id
),
summary AS (
    -- Summarise per group
    SELECT
        policy_status,
        ARRAY_SORT(COLLECT_SET(policy_status)) AS policy_status_list,
        SUM(CASE WHEN holder_region <> 'North' THEN 1 ELSE 0 END) AS n_holder_region_north,
        COUNT(*) AS cnt
    FROM enriched
    GROUP BY policy_status
    HAVING COUNT(*) >= 2
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(n_holder_region_north - AVG(n_holder_region_north) OVER (PARTITION BY cnt), 2) AS n_holder_region_north_vs_cnt_avg,
        ROW_NUMBER() OVER (ORDER BY cnt, policy_status) AS cnt_seq,
        NTILE(5) OVER (ORDER BY cnt, policy_status) AS cnt_quintile,
        ROUND(CUME_DIST() OVER w_n_holder_region_north_by_cnt, 3) AS cnt_cume_dist,
        NTILE(4) OVER w_n_holder_region_north_by_cnt_2 AS cnt_quartile,
        MAX(cnt) OVER (PARTITION BY n_holder_region_north ORDER BY cnt, policy_status ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS cnt_max_3
    FROM summary
    WINDOW w_n_holder_region_north_by_cnt AS (PARTITION BY n_holder_region_north ORDER BY cnt), w_n_holder_region_north_by_cnt_2 AS (PARTITION BY n_holder_region_north ORDER BY cnt, policy_status)
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        policy_status,
        n_holder_region_north,
        cnt_seq,
        cnt_cume_dist,
        cnt_max_3,
        MIN(cnt_max_3) OVER (PARTITION BY policy_status, cnt_quintile) AS policy_status_cnt_quintile_min_cnt_max_3,
        LAG(cnt_quartile, 3, 0) OVER (PARTITION BY cnt_seq ORDER BY n_holder_region_north_vs_cnt_avg DESC, policy_status) AS prev3_cnt_quartile,
        ROW_NUMBER() OVER w_cnt_seq_by_n_holder_region_n AS n_holder_region_north_vs_cnt_avg_seq
    FROM sequenced
    WINDOW w_cnt_seq_by_n_holder_region_n AS (PARTITION BY cnt_seq ORDER BY n_holder_region_north_vs_cnt_avg DESC, policy_status)
),
first_rows AS (
    -- Top 5 per group
    SELECT *
    FROM windowed
    WHERE cnt_seq <= 5
)
SELECT *
FROM (
    SELECT cnt_seq, prev3_cnt_quartile, policy_status, policy_status_cnt_quintile_min_cnt_max_3
    FROM first_rows
) src
PIVOT (
    SUM(policy_status_cnt_quintile_min_cnt_max_3) AS amount, MAX(policy_status_cnt_quintile_min_cnt_max_3) AS max_v
    FOR policy_status IN ('CANCELLED', 'EXPIRED', 'LAPSED', 'ACTIVE')
)
