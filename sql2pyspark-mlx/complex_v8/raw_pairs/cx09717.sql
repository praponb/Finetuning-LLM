WITH scoped_holders AS (
    -- Filter policyholders
    SELECT
        id,
        holder_name,
        region,
        joined_on,
        segment,
        LOWER(REGEXP_REPLACE(TRIM(region), ' ', '_')) AS region_slug
    FROM policyholders
    WHERE holder_name IS NOT NULL
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAST_VALUE(joined_on) IGNORE NULLS OVER (PARTITION BY region, segment ORDER BY joined_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_joined_on,
        DATEDIFF(joined_on, LAG(joined_on) OVER (PARTITION BY region, DATE_TRUNC('MONTH', joined_on) ORDER BY joined_on, id)) AS days_since_prev_joined_on,
        RANK() OVER (PARTITION BY region, segment ORDER BY joined_on DESC) AS joined_on_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY region ORDER BY joined_on), 3) AS joined_on_cume_dist,
        ROUND(CUME_DIST() OVER w_segment_region_by_joined_on, 3) AS joined_on_cume_dist_2,
        LAST_VALUE(joined_on) OVER (PARTITION BY segment, region ORDER BY joined_on NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_joined_on,
        DATEDIFF(joined_on, LAG(joined_on) OVER (PARTITION BY region ORDER BY joined_on, id)) AS days_since_prev_joined_on_2,
        FIRST_VALUE(joined_on) IGNORE NULLS OVER (PARTITION BY region, DATE_TRUNC('MONTH', joined_on) ORDER BY joined_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_joined_on,
        ROUND(CUME_DIST() OVER (PARTITION BY segment ORDER BY joined_on DESC NULLS FIRST, id), 3) AS joined_on_cume_dist_4,
        LEAD(joined_on) OVER w_segment_by_joined_on AS next_joined_on,
        ROW_NUMBER() OVER (PARTITION BY region, DATE_TRUNC('MONTH', joined_on) ORDER BY joined_on, id) AS joined_on_seq,
        DENSE_RANK() OVER (PARTITION BY region, segment ORDER BY joined_on DESC) AS joined_on_dense_rank,
        DATEDIFF(joined_on, LAG(joined_on) OVER w_segment_by_joined_on) AS days_since_prev_joined_on_3
    FROM scoped_holders
    WINDOW w_segment_region_by_joined_on AS (PARTITION BY segment, region ORDER BY joined_on NULLS LAST), w_segment_by_joined_on AS (PARTITION BY segment ORDER BY joined_on DESC NULLS FIRST, id)
)
-- Final projection
SELECT
    id,
    holder_name,
    region,
    joined_on,
    segment,
    last_known_joined_on,
    days_since_prev_joined_on,
    joined_on_rank,
    joined_on_cume_dist,
    joined_on_cume_dist_2,
    days_since_prev_joined_on_2,
    next_joined_on,
    joined_on_seq,
    joined_on_dense_rank,
    DATEDIFF(last_joined_on, last_known_joined_on) AS days_last_known_joined_on_to_last_joined_on
FROM with_windows
ORDER BY joined_on_dense_rank, next_joined_on NULLS LAST
