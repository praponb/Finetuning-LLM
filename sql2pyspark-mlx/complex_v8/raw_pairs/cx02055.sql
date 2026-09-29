WITH recent_holders AS (
    -- Scope the policyholders rows
    SELECT
        id,
        holder_name,
        region,
        joined_on,
        CONCAT(region, '#', CAST(id AS STRING)) AS region_ref
    FROM policyholders
    WHERE joined_on BETWEEN DATE '2024-03-22' AND DATE '2025-07-03'
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        CONCAT_WS(' / ', region, holder_name) AS region_holder_name_label,
        LENGTH(TRIM(region)) AS region_len
    FROM recent_holders
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        region,
        region_holder_name_label,
        region_len,
        ROUND(region_len / NULLIF(SUM(region_len) OVER w_region_region_len, 0), 4) AS region_len_share_of_region_region_len,
        DENSE_RANK() OVER (PARTITION BY MONTH(joined_on), region ORDER BY region_len NULLS LAST, id) AS region_len_dense_rank,
        SUM(region_len) OVER (PARTITION BY MONTH(joined_on), region ORDER BY region_len NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS region_len_sum_2,
        MAX(region_len) OVER (PARTITION BY MONTH(joined_on), region ORDER BY region_len NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS region_len_max_3_prior,
        LAG(region_len, 2, 0) OVER (PARTITION BY MONTH(joined_on), region ORDER BY region_len NULLS LAST, id) AS prev2_region_len,
        LAST_VALUE(region_len) OVER (PARTITION BY MONTH(joined_on), region ORDER BY region_len NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_region_len
    FROM scored
    WINDOW w_region_region_len AS (PARTITION BY region, region_len)
),
policy_stats AS (
    -- Per-holder policies metrics
    SELECT holder_id, COUNT(*) AS policy_count
    FROM policies
    GROUP BY holder_id
),
with_stats AS (
    -- Attach policies metrics
    SELECT
        s.*,
        COALESCE(p.policy_count, 0) AS policy_count
    FROM ranked s
    LEFT JOIN policy_stats p ON p.holder_id = s.id
)
-- Final projection
SELECT
    id,
    region,
    region_holder_name_label,
    region_len,
    region_len_sum_2,
    region_len_max_3_prior,
    prev2_region_len,
    last_region_len
FROM with_stats
ORDER BY id DESC
LIMIT 5
