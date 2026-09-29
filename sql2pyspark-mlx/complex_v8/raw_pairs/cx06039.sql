WITH prepared_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        ROUND(premium * 1.5, 2) AS premium_adj
    FROM policies
    WHERE product = 'LIFE'
),
joined AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.status,
        s.premium_adj,
        p.region AS holder_region,
        p.segment AS holder_segment
    FROM prepared_policys s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        premium,
        premium_adj,
        ROW_NUMBER() OVER w_all_by_premium AS rn,
        SIZE(COLLECT_SET(holder_id) OVER w_status_holder_region) AS distinct_holder_id_in_status_holder_region,
        ROUND(CUME_DIST() OVER (PARTITION BY holder_id, holder_segment ORDER BY premium, id), 3) AS premium_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY holder_id, holder_segment ORDER BY premium, id) AS seq_no,
        LAG(premium_adj, 3) OVER w_status_by_premium_adj AS prev3_premium_adj,
        MIN(premium_adj) OVER (PARTITION BY holder_id, holder_segment ORDER BY premium_adj, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_premium_adj_so_far,
        MAX(premium) OVER (PARTITION BY status, holder_region) AS status_holder_region_max_premium,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY status, holder_segment)) AS distinct_holder_id_in_status_holder_segment,
        premium - LAG(premium) OVER (ORDER BY premium DESC, id) AS premium_change,
        ROUND(premium - AVG(premium) OVER (PARTITION BY status, holder_region), 2) AS premium_vs_status_holder_region_avg,
        LEAD(premium_adj, 1, 0) OVER w_status_by_premium_adj AS next_premium_adj
    FROM joined
    WINDOW w_all_by_premium AS (ORDER BY premium DESC, id), w_status_holder_region AS (PARTITION BY status, holder_region), w_status_by_premium_adj AS (PARTITION BY status ORDER BY premium_adj ASC NULLS LAST, id)
),
enriched AS (
    -- Add calculated columns
    SELECT *, seq_no + premium_adj AS seq_no_plus_premium_adj
    FROM sequenced
)
-- Final projection
SELECT
    id,
    premium,
    distinct_holder_id_in_status_holder_region,
    premium_cume_dist,
    prev3_premium_adj,
    premium_change,
    premium_vs_status_holder_region_avg,
    next_premium_adj,
    seq_no_plus_premium_adj,
    FLOOR(min_premium_adj_so_far / 10000) * 10000 AS min_premium_adj_so_far_bucket,
    next_premium_adj + seq_no AS next_premium_adj_plus_seq_no
FROM enriched
ORDER BY seq_no_plus_premium_adj, premium_cume_dist
