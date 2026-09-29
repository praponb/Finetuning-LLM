WITH cleaned AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status,
        LOWER(TRIM(product)) AS product_clean
    FROM policies
    WHERE start_date < DATE '2025-05-30'
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.end_date,
        s.status,
        s.product_clean,
        p.segment AS holder_segment
    FROM cleaned s
    JOIN policyholders p ON p.id = s.holder_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        holder_id,
        end_date,
        product_clean,
        holder_segment,
        ROW_NUMBER() OVER w_product_by_end_date AS rn,
        ROUND(PERCENT_RANK() OVER (PARTITION BY product ORDER BY end_date), 3) AS end_date_percent_rank
    FROM joined
    WINDOW w_product_by_end_date AS (PARTITION BY product ORDER BY end_date, id)
),
summary AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', end_date) AS end_date_month,
        holder_id,
        COUNT(*) AS n_rows,
        SUM(end_date_percent_rank) AS total_end_date_percent_rank,
        COUNT(DISTINCT holder_segment) AS distinct_holder_segments,
        ARRAY_SORT(COLLECT_SET(holder_segment)) AS holder_segment_list,
        ROUND(AVG(end_date_percent_rank), 2) AS avg_end_date_percent_rank,
        ROUND(SUM(rn) / NULLIF(SUM(end_date_percent_rank), 0), 3) AS rn_to_end_date_percent_rank_ratio
    FROM with_windows
    GROUP BY 1, 2
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.end_date_month,
        s.holder_id,
        s.n_rows,
        s.total_end_date_percent_rank,
        s.distinct_holder_segments,
        s.holder_segment_list,
        s.avg_end_date_percent_rank,
        s.rn_to_end_date_percent_rank_ratio,
        p.joined_on AS holder_joined_on
    FROM summary s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
with_children AS (
    -- One row per policy
    SELECT
        s.end_date_month,
        s.holder_id,
        s.total_end_date_percent_rank,
        s.distinct_holder_segments,
        s.holder_segment_list,
        s.avg_end_date_percent_rank,
        s.rn_to_end_date_percent_rank_ratio,
        s.holder_joined_on,
        p.id AS policy_id,
        p.premium AS policy_premium,
        p.start_date AS policy_start_date,
        p.status AS policy_status,
        p.product AS policy_product
    FROM combined_details s
    LEFT JOIN policies p ON p.holder_id = s.holder_id
),
combined_details_2 AS (
    -- Join policyholders
    SELECT
        s.end_date_month,
        s.holder_id,
        s.total_end_date_percent_rank,
        s.distinct_holder_segments,
        s.avg_end_date_percent_rank,
        s.rn_to_end_date_percent_rank_ratio,
        s.policy_id,
        s.policy_premium,
        s.policy_start_date,
        s.policy_status,
        s.policy_product,
        p.region AS holder_region
    FROM with_children s
    JOIN policyholders p ON p.id = s.holder_id
)
SELECT
    end_date_month,
    holder_id,
    total_end_date_percent_rank,
    distinct_holder_segments,
    avg_end_date_percent_rank,
    rn_to_end_date_percent_rank_ratio,
    policy_id,
    policy_premium,
    policy_start_date,
    policy_status,
    policy_product,
    holder_region
FROM combined_details_2
ORDER BY distinct_holder_segments, policy_start_date DESC
