WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        EXPLODE(coupon_codes) AS tag
    FROM carts
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LENGTH(tag) tag_length,
        CASE WHEN tag IN ('VIP', 'BUNDLE') THEN 'Core' ELSE 'Other' END tag_group
    FROM lines
    WHERE created_at > TIMESTAMP '2025-03-01 11:00:00'
      AND tag IN ('VIP', 'BUNDLE')
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        tag_length,
        tag_group,
        PERCENT_RANK() OVER (PARTITION BY tag ORDER BY tag_length DESC) tag_length_percent_rank,
        RANK() OVER (ORDER BY tag_length DESC NULLS LAST) tag_length_rank
    FROM valid
),
enriched AS (
    -- Ranking and gap features
    SELECT *, DENSE_RANK() OVER (ORDER BY tag_length_percent_rank DESC) tag_length_percent_rank_dense_rank
    FROM scored
    WHERE tag_length >= 54
),
scored_carts AS (
    -- Running and moving aggregates
    SELECT
        tag_length_percent_rank,
        tag_length_rank,
        tag_length_percent_rank_dense_rank,
        RANK() OVER (ORDER BY tag_length_percent_rank DESC NULLS LAST) tag_length_percent_rank_rank,
        PERCENT_RANK() OVER (ORDER BY tag_length_rank DESC) tag_length_rank_percent_rank,
        RANK() OVER (ORDER BY tag_length NULLS FIRST) tag_length_rank_2,
        RANK() OVER (ORDER BY tag_length DESC NULLS LAST) tag_length_rank_3
    FROM enriched
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        tag_length_rank_percent_rank,
        tag_length_rank_3,
        PERCENT_RANK() OVER (ORDER BY tag_length_percent_rank DESC) tag_length_percent_rank_percent_rank
    FROM scored_carts
),
daily AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN tag_length_rank_percent_rank >= 0.61 THEN 'PREMIUM' ELSE 'STANDARD' END AS tag_length_rank_percent_rank_tier,
        APPROX_PERCENTILE(tag_length_rank_3, ARRAY(0.25, 0.5, 0.75)) AS tag_length_rank_3_quartiles,
        PERCENTILE_APPROX(tag_length_percent_rank_percent_rank, 0.5) AS tag_length_percent_rank_percent_rank_p50,
        APPROX_PERCENTILE(tag_length_rank_percent_rank, 0.5) AS tag_length_rank_percent_rank_p50,
        ROUND(AVG(tag_length_percent_rank_percent_rank), 2) AS avg_tag_length_percent_rank_percent_rank,
        GROUPING(CASE WHEN tag_length_rank_percent_rank >= 0.61 THEN 'PREMIUM' ELSE 'STANDARD' END) AS gid
    FROM with_windows
    GROUP BY CUBE (CASE WHEN tag_length_rank_percent_rank >= 0.61 THEN 'PREMIUM' ELSE 'STANDARD' END)
)
SELECT
    tag_length_rank_3_quartiles,
    tag_length_rank_percent_rank_p50,
    avg_tag_length_percent_rank_percent_rank,
    gid
FROM daily;
