WITH filtered_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        LENGTH(TRIM(status)) AS status_len
    FROM policies
    WHERE start_date BETWEEN DATE '2024-03-27' AND DATE '2025-05-28'
),
latest AS (
    -- Keep the latest row per holder_id
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status,
        status_len
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY holder_id ORDER BY premium DESC, id DESC) AS row_num
        FROM filtered_policys
    ) ranked
    WHERE row_num = 1
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        start_date,
        status,
        status_len,
        MAX(premium) OVER w_status_len_status AS status_len_status_max_premium,
        LEAD(start_date) OVER (PARTITION BY status ORDER BY premium DESC, id) AS next_start_date,
        ROUND(status_len / NULLIF(SUM(status_len) OVER (PARTITION BY status_len, status), 0), 4) AS status_len_share_of_status_len_status,
        DENSE_RANK() OVER (PARTITION BY status, holder_id ORDER BY end_date) AS end_date_dense_rank,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY status_len, status)) AS distinct_holder_id_in_status_len_status
    FROM latest
    WINDOW w_status_len_status AS (PARTITION BY status_len, status)
),
detail_rows_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.start_date,
        s.status,
        s.status_len,
        s.next_start_date,
        s.status_len_share_of_status_len_status,
        s.distinct_holder_id_in_status_len_status,
        c.id AS claim_id,
        c.claim_type,
        c.reported_at AS claim_reported_at,
        c.adjuster_id AS claim_adjuster_id,
        c.status AS claim_status
    FROM sequenced s
    LEFT JOIN claims c ON c.policy_id = s.id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        start_date,
        status,
        status_len,
        next_start_date,
        distinct_holder_id_in_status_len_status,
        claim_id,
        claim_reported_at,
        claim_adjuster_id,
        SUM(status_len) OVER (PARTITION BY status, MONTH(next_start_date)) AS status_next_start_date_month_sum_status_len,
        NTH_VALUE(next_start_date, 2) OVER (PARTITION BY status_len, distinct_holder_id_in_status_len_status ORDER BY next_start_date, id, claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_next_start_date,
        ROUND(status_len - AVG(status_len) OVER (PARTITION BY status, MONTH(next_start_date)), 2) AS status_len_vs_status_next_start_date_month_avg,
        ROUND(AVG(status_len) OVER w_status_next_start_date_month, 2) AS status_next_start_date_month_avg_status_len,
        ROUND(status_len / NULLIF(SUM(status_len) OVER (PARTITION BY status, MONTH(next_start_date)), 0), 4) AS status_len_share_of_status_next_start_date_month
    FROM detail_rows_claims
    WINDOW w_status_next_start_date_month AS (PARTITION BY status, MONTH(next_start_date))
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        status,
        status_len,
        next_start_date,
        distinct_holder_id_in_status_len_status,
        claim_id,
        claim_reported_at,
        claim_adjuster_id,
        second_next_start_date,
        status_len_share_of_status_next_start_date_month,
        NTH_VALUE(second_next_start_date, 2) OVER (ORDER BY status_len_vs_status_next_start_date_month_avg, id, claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_second_next_start_date,
        MIN(status_len) OVER (ORDER BY status_len_vs_status_next_start_date_month_avg, id, claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_status_len_so_far
    FROM with_trends
),
joined_claim AS (
    -- Attach claim attributes
    SELECT
        s.id,
        s.status,
        s.status_len,
        s.next_start_date,
        s.distinct_holder_id_in_status_len_status,
        s.claim_id,
        s.claim_reported_at,
        s.claim_adjuster_id,
        s.second_next_start_date,
        s.status_len_share_of_status_next_start_date_month,
        s.second_second_next_start_date,
        c.policy_id AS claim_policy_id,
        c.status AS claim_status,
        c.adjuster_id AS claim_adjuster_id_2
    FROM windowed s
    JOIN claims c ON c.id = s.claim_id AND c.claim_date BETWEEN DATE '2024-06-07' AND DATE '2025-11-12'
    WHERE c.status IN ('PAID', 'OPEN', 'APPROVED')
)
-- Final projection
SELECT
    id,
    status,
    next_start_date,
    distinct_holder_id_in_status_len_status,
    claim_id,
    claim_reported_at,
    claim_adjuster_id,
    second_next_start_date,
    status_len_share_of_status_next_start_date_month,
    second_second_next_start_date,
    claim_policy_id,
    claim_status
FROM joined_claim
ORDER BY status_len_share_of_status_next_start_date_month DESC NULLS FIRST, id, claim_id
LIMIT 5
