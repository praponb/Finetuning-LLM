WITH scoped_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        ADD_MONTHS(start_date, 12) AS start_date_plus_12m,
        DATEDIFF(start_date, end_date) AS days_end_date_to_start_date
    FROM policies
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.status,
        s.start_date_plus_12m,
        s.days_end_date_to_start_date,
        p.joined_on AS holder_joined_on,
        p.segment AS holder_segment
    FROM scoped_policys s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
deduped AS (
    -- Keep the latest row per holder_id, status
    SELECT
        id,
        holder_id,
        status,
        start_date_plus_12m,
        days_end_date_to_start_date,
        holder_joined_on,
        holder_segment
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY holder_id, status ORDER BY days_end_date_to_start_date DESC, id DESC) AS recency_rank
        FROM with_details
    ) ranked
    WHERE recency_rank = 1
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        holder_id,
        start_date_plus_12m,
        holder_joined_on,
        holder_segment,
        ROUND(PERCENT_RANK() OVER (ORDER BY days_end_date_to_start_date DESC, id), 3) AS days_end_date_to_start_date_percent_rank,
        LAG(start_date_plus_12m) OVER w_all_by_days_end_date_to_star AS prev_start_date_plus_12m
    FROM deduped
    WINDOW w_all_by_days_end_date_to_star AS (ORDER BY days_end_date_to_start_date DESC, id)
),
with_parent_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.start_date_plus_12m,
        s.holder_joined_on,
        s.holder_segment,
        s.days_end_date_to_start_date_percent_rank,
        s.prev_start_date_plus_12m,
        p.joined_on AS holder_joined_on_2,
        p.region AS holder_region,
        p.segment AS holder_segment_2
    FROM ranked s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
detail_rows AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.holder_joined_on,
        s.holder_segment,
        s.holder_joined_on_2,
        s.holder_region,
        s.holder_segment_2,
        c.id AS claim_id,
        c.adjuster_id AS claim_adjuster_id,
        c.claimed_amount AS claim_claimed_amount
    FROM with_parent_holder s
    JOIN claims c ON c.policy_id = s.id AND c.claimed_amount BETWEEN 1705.6 AND 43724.9
)
SELECT *
FROM (
    SELECT holder_joined_on, claim_adjuster_id, holder_segment, claim_claimed_amount
    FROM detail_rows
) src
PIVOT (
    COUNT(claim_claimed_amount) AS count_v, MAX(claim_claimed_amount) AS max_v
    FOR holder_segment IN ('SME', 'VIP')
)
