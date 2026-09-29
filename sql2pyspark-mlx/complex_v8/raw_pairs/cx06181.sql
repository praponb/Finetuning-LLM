WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        reported_at,
        ROUND(approved_amount * 0.9, 2) AS approved_amount_adj,
        LOWER(REGEXP_REPLACE(TRIM(claim_type), ' ', '_')) AS claim_type_slug
    FROM claims
),
joined AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.approved_amount,
        s.reported_at,
        s.approved_amount_adj,
        s.claim_type_slug,
        a.adjuster_name
    FROM valid s
    JOIN adjusters a ON a.id = s.adjuster_id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        approved_amount,
        reported_at,
        approved_amount_adj,
        LAST_VALUE(reported_at) IGNORE NULLS OVER (PARTITION BY adjuster_id ORDER BY approved_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_reported_at,
        SUM(approved_amount) OVER (PARTITION BY policy_id) AS policy_id_sum_approved_amount,
        approved_amount_adj - LAG(approved_amount_adj) OVER w_adjuster_id_by_approved_amou_2 AS approved_amount_adj_change,
        COUNT(*) OVER (PARTITION BY TO_DATE(reported_at)) AS rows_in_reported_at_to_date,
        NTILE(10) OVER w_adjuster_id_by_approved_amou_2 AS approved_amount_decile,
        MAX(approved_amount_adj) OVER (ORDER BY claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_approved_amount_adj_so_far,
        ROUND(approved_amount_adj - AVG(approved_amount_adj) OVER w_policy_id, 2) AS approved_amount_adj_vs_policy_id_avg,
        ROUND(AVG(approved_amount) OVER (PARTITION BY TO_DATE(reported_at)), 2) AS reported_at_to_date_avg_approved_amount,
        NTILE(4) OVER (ORDER BY claim_date, id) AS claim_date_quartile
    FROM joined
    WINDOW w_adjuster_id_by_approved_amou_2 AS (PARTITION BY adjuster_id ORDER BY approved_amount, id), w_policy_id AS (PARTITION BY policy_id)
)
SELECT
    id,
    policy_id,
    claim_date,
    reported_at,
    approved_amount_adj,
    last_known_reported_at,
    policy_id_sum_approved_amount,
    rows_in_reported_at_to_date,
    approved_amount_decile,
    max_approved_amount_adj_so_far,
    approved_amount_adj_vs_policy_id_avg,
    claim_date_quartile,
    DATE_FORMAT(reported_at, 'yyyy-MM-dd HH:00') AS reported_at_label
FROM ranked
ORDER BY rows_in_reported_at_to_date, approved_amount_decile DESC
