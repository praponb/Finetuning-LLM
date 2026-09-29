WITH base_holders AS (
    -- Filter policyholders
    SELECT
        id,
        holder_name,
        region,
        joined_on,
        segment,
        CASE WHEN segment IN ('VIP', 'RETAIL') THEN 'group_a' WHEN segment = 'SME' THEN 'group_b' ELSE 'other' END AS segment_group,
        CONCAT_WS(': ', region, holder_name) AS region_holder_name_label,
        CASE WHEN segment IN ('SME', 'RETAIL') THEN 'group_a' WHEN segment = 'VIP' THEN 'group_b' ELSE 'other' END AS segment_group_2
    FROM policyholders
    WHERE holder_name IS NOT NULL
),
rollup_base AS (
    -- Group-level totals
    SELECT
        region,
        segment,
        COUNT(*) AS row_count,
        COUNT(DISTINCT segment_group_2) AS distinct_segment_group_2s,
        ROUND(100 * COUNT(CASE WHEN segment IN ('RETAIL', 'SME') THEN 1 END) / COUNT(*), 1) AS pct_segment_in_retail_sme,
        SUM(CASE WHEN region <> 'North' THEN 1 ELSE 0 END) AS n_region_north,
        COUNT(DISTINCT segment) AS distinct_segments
    FROM base_holders
    GROUP BY region, segment
    HAVING COUNT(*) >= 2
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM rollup_base
    WHERE n_region_north < (SELECT MAX(n_region_north) FROM rollup_base)
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        region,
        segment,
        row_count,
        pct_segment_in_retail_sme,
        n_region_north,
        distinct_segments,
        SIZE(COLLECT_SET(segment) OVER (PARTITION BY distinct_segments)) AS distinct_segment_in_distinct_segments,
        MAX(pct_segment_in_retail_sme) OVER (PARTITION BY segment ORDER BY n_region_north ASC, region, segment ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS pct_segment_in_retail_sme_max_2,
        ROW_NUMBER() OVER (PARTITION BY row_count ORDER BY n_region_north, region, segment) AS seq_no
    FROM qualified
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(region) OVER (PARTITION BY n_region_north, row_count)) AS distinct_region_in_n_region_north_row_count,
        DENSE_RANK() OVER (PARTITION BY segment, distinct_segment_in_distinct_segments ORDER BY n_region_north) AS n_region_north_dense_rank,
        ROUND(distinct_segments / NULLIF(SUM(distinct_segments) OVER w_n_region_north_row_count, 0), 4) AS distinct_segments_share_of_n_region_north_row_co,
        ROUND(n_region_north / NULLIF(SUM(n_region_north) OVER w_n_region_north_row_count, 0), 4) AS n_region_north_share_of_n_region_north_row_count,
        ROUND(row_count - AVG(row_count) OVER (PARTITION BY n_region_north, row_count), 2) AS row_count_vs_n_region_north_row_count_avg
    FROM with_windows
    WINDOW w_n_region_north_row_count AS (PARTITION BY n_region_north, row_count)
)
SELECT
    region,
    segment,
    row_count,
    pct_segment_in_retail_sme,
    n_region_north,
    distinct_segment_in_distinct_segments,
    pct_segment_in_retail_sme_max_2,
    seq_no,
    distinct_region_in_n_region_north_row_count,
    n_region_north_dense_rank,
    n_region_north_share_of_n_region_north_row_count,
    row_count_vs_n_region_north_row_count_avg,
    LENGTH(TRIM(segment)) AS segment_len,
    COALESCE(segment, 'UNKNOWN') AS segment_or_unknown
FROM windowed
WHERE pct_segment_in_retail_sme_max_2 <= 195.0
