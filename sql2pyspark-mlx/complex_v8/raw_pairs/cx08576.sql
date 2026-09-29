WITH prepared AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        status,
        CASE WHEN product IN ('TRAVEL', 'HEALTH') THEN 'group_a' WHEN product = 'AUTO' THEN 'group_b' ELSE 'other' END AS product_group,
        ADD_MONTHS(start_date, 3) AS start_date_plus_3m
    FROM policies
    WHERE product IS NOT NULL
      AND status IN ('LAPSED', 'CANCELLED', 'EXPIRED')
),
line_level_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.status,
        s.product_group,
        s.start_date_plus_3m,
        c.id AS claim_id,
        c.adjuster_id AS claim_adjuster_id,
        c.claim_date,
        c.claim_type
    FROM prepared s
    LEFT JOIN claims c ON c.policy_id = s.id AND c.claim_type = 'collision'
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        NTH_VALUE(claim_date, 2) OVER (PARTITION BY status ORDER BY start_date_plus_3m DESC, id, claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claim_date,
        LAG(start_date_plus_3m) OVER (PARTITION BY holder_id ORDER BY start_date_plus_3m DESC, id, claim_id) AS prev_start_date_plus_3m,
        ROUND(PERCENT_RANK() OVER (PARTITION BY holder_id ORDER BY start_date_plus_3m DESC), 3) AS start_date_plus_3m_percent_rank,
        LAG(claim_date, 2) OVER w_status_by_start_date_plus_3m_2 AS prev2_claim_date,
        ROUND(CUME_DIST() OVER (PARTITION BY holder_id ORDER BY start_date_plus_3m DESC, id, claim_id), 3) AS start_date_plus_3m_cume_dist
    FROM line_level_claims
    WINDOW w_status_by_start_date_plus_3m_2 AS (PARTITION BY status ORDER BY start_date_plus_3m DESC, id, claim_id)
),
profiled AS (
    -- Attach policies metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM policies p WHERE p.holder_id = s.holder_id) AS policy_count,
        (SELECT COUNT(*) FILTER (WHERE p.status = 'LAPSED') FROM policies p WHERE p.holder_id = s.holder_id) AS policy_status_count,
        (SELECT COUNT(*) FROM policies p WHERE p.holder_id = s.holder_id) AS policy_count_2
    FROM metrics s
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        claim_type,
        product_group,
        claim_id,
        ARRAY_SORT(COLLECT_SET(status)) AS status_list,
        COUNT(*) AS n_rows
    FROM profiled
    GROUP BY claim_type, product_group, claim_id
)
-- Final projection
SELECT
    claim_type,
    product_group,
    claim_id,
    status_list,
    n_rows,
    COALESCE(claim_type, 'UNKNOWN') AS claim_type_or_unknown,
    LOWER(REGEXP_REPLACE(TRIM(product_group), ' ', '_')) AS product_group_slug
FROM per_group
