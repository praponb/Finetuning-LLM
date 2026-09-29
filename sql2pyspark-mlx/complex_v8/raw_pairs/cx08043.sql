WITH valid_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        COALESCE(product, 'UNKNOWN') AS product_or_unknown,
        ABS(premium - 3465.4) AS premium_distance
    FROM policies
    WHERE (start_date BETWEEN DATE '2024-07-19' AND DATE '2025-03-24' OR premium >= 2013.8)
),
with_children AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        s.status,
        s.product_or_unknown,
        c.id AS claim_id,
        c.claim_type,
        c.status AS claim_status,
        c.claim_date
    FROM valid_policys s
    LEFT JOIN claims c ON c.policy_id = s.id
),
combined_details_claim AS (
    -- Join claims
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        s.product_or_unknown,
        s.claim_id,
        s.claim_type,
        s.claim_status,
        c.status AS claim_status_2,
        c.reported_at AS claim_reported_at
    FROM with_children s
    JOIN claims c ON c.id = s.claim_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROW_NUMBER() OVER w_holder_id_product_or_unknown AS seq_no,
        FIRST_VALUE(claim_reported_at) OVER (PARTITION BY holder_id ORDER BY start_date, id, claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_claim_reported_at,
        LAST_VALUE(start_date) OVER (PARTITION BY claim_status ORDER BY claim_reported_at DESC, id, claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_start_date,
        ROUND(PERCENT_RANK() OVER (PARTITION BY holder_id ORDER BY start_date), 3) AS start_date_percent_rank,
        NTH_VALUE(start_date, 2) OVER (PARTITION BY holder_id, product_or_unknown ORDER BY start_date DESC NULLS FIRST, id, claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date
    FROM combined_details_claim
    WINDOW w_holder_id_product_or_unknown AS (PARTITION BY holder_id, product_or_unknown ORDER BY start_date DESC NULLS FIRST, id, claim_id)
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(seq_no) OVER (PARTITION BY seq_no ORDER BY second_start_date NULLS LAST, id, claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_seq_no,
        ROW_NUMBER() OVER (ORDER BY claim_reported_at ASC, id, claim_id) AS claim_reported_at_seq,
        ROUND(seq_no / NULLIF(SUM(seq_no) OVER w_claim_type, 0), 4) AS seq_no_share_of_claim_type,
        ROUND(start_date_percent_rank / NULLIF(SUM(start_date_percent_rank) OVER (PARTITION BY claim_type), 0), 4) AS start_date_percent_rank_share_of_claim_type,
        ROUND(AVG(seq_no) OVER (ORDER BY claim_reported_at ASC, id, claim_id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS seq_no_avg_2
    FROM metrics
    WINDOW w_claim_type AS (PARTITION BY claim_type)
),
combined_details_claim_2 AS (
    -- Attach claim attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        s.product_or_unknown,
        s.claim_id,
        s.claim_type,
        s.claim_status,
        s.claim_reported_at,
        s.seq_no,
        s.start_date_percent_rank,
        s.second_start_date,
        s.first_seq_no,
        s.claim_reported_at_seq,
        s.seq_no_share_of_claim_type,
        s.start_date_percent_rank_share_of_claim_type,
        s.seq_no_avg_2,
        c.claim_type AS claim_claim_type,
        c.claim_date,
        c.reported_at AS claim_reported_at_2
    FROM windowed s
    LEFT JOIN claims c ON c.id = s.claim_id
),
windowed_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        product,
        start_date,
        claim_id,
        claim_type,
        claim_reported_at,
        seq_no,
        start_date_percent_rank,
        claim_reported_at_seq,
        start_date_percent_rank_share_of_claim_type,
        seq_no_avg_2,
        claim_claim_type,
        claim_date,
        claim_reported_at_2,
        SUM(seq_no_share_of_claim_type) OVER (PARTITION BY claim_id ORDER BY seq_no NULLS LAST, id, claim_id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS seq_no_share_of_claim_type_sum_6_prior,
        DATEDIFF(start_date, LAG(start_date) OVER (PARTITION BY claim_id ORDER BY seq_no NULLS LAST, id, claim_id)) AS days_since_prev_start_date,
        LAST_VALUE(second_start_date) IGNORE NULLS OVER (ORDER BY start_date_percent_rank_share_of_claim_type, id, claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_second_start_date,
        LEAD(seq_no_avg_2) OVER (PARTITION BY claim_id ORDER BY seq_no NULLS LAST, id, claim_id) AS next_seq_no_avg_2,
        ROUND(CUME_DIST() OVER (ORDER BY start_date_percent_rank_share_of_claim_type, id, claim_id), 3) AS start_date_percent_rank_share_of_claim_type_cume,
        SIZE(COLLECT_SET(claim_id) OVER (PARTITION BY claim_type, DATE_TRUNC('MONTH', second_start_date))) AS distinct_claim_id_in_claim_type_second_start_dat
    FROM combined_details_claim_2
)
SELECT
    id,
    claim_id,
    claim_type,
    seq_no,
    claim_reported_at_seq,
    start_date_percent_rank_share_of_claim_type,
    seq_no_avg_2,
    claim_date,
    claim_reported_at_2,
    days_since_prev_start_date,
    last_known_second_start_date,
    next_seq_no_avg_2,
    CASE WHEN claim_claim_type IN ('flood', 'delay') THEN 'group_a' WHEN claim_claim_type = 'collision' THEN 'group_b' ELSE 'other' END AS claim_claim_type_group,
    MONTH(claim_reported_at) AS claim_reported_at_month
FROM windowed_2
ORDER BY claim_reported_at_seq, last_known_second_start_date DESC
