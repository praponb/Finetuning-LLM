WITH lines AS (
    SELECT
        id,
        created_at,
        EXPLODE_OUTER(coupon_codes) AS tag
    FROM carts
),
valid AS (
    SELECT
        *,
        DATE_FORMAT(created_at, 'MMM yyyy') created_at_label,
        COALESCE(tag, 'N/A') tag_clean
    FROM lines
    WHERE created_at >= TO_TIMESTAMP('2025-03-02 03:00:00')
      AND created_at >= '2025-06-09'
),
filtered AS (
    SELECT *, LENGTH(tag) tag_length
    FROM valid
    WHERE created_at >= DATE '2025-03-01'
),
scored AS (
    SELECT
        id,
        tag,
        created_at_label,
        tag_clean,
        MAX(tag_length) OVER (PARTITION BY tag ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) tag_length_max_15m
    FROM filtered
),
enriched AS (
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY tag ORDER BY tag_length_max_15m NULLS FIRST) tag_length_max_15m_percent_rank,
        RANK() OVER (ORDER BY tag_length_max_15m DESC NULLS LAST) tag_length_max_15m_rank
    FROM scored
),
scored_carts AS (
    SELECT
        id,
        tag,
        created_at_label,
        tag_clean,
        tag_length_max_15m,
        tag_length_max_15m_percent_rank,
        RANK() OVER (PARTITION BY tag ORDER BY tag_length_max_15m NULLS FIRST) tag_length_max_15m_rank_2,
        RANK() OVER (PARTITION BY tag ORDER BY tag_length_max_15m DESC NULLS LAST) tag_length_max_15m_rank_3,
        DENSE_RANK() OVER (PARTITION BY tag ORDER BY tag_length_max_15m_rank DESC) tag_length_max_15m_rank_dense_rank
    FROM enriched
),
cleaned AS (
    SELECT
        id,
        tag,
        tag_length_max_15m,
        tag_length_max_15m_rank_2,
        tag_length_max_15m_rank_3,
        tag_length_max_15m_rank_dense_rank,
        ROUND(tag_length_max_15m_percent_rank / tag_length_max_15m_rank_3, 2) tag_length_max_15m_percent_rank_per_tag_length_max_15m_rank_3,
        COALESCE(tag, 'NONE') tag_clean_2
    FROM scored_carts
    WHERE tag_length_max_15m IS NULL
)
SELECT
    tag,
    ROUND(SUM(tag_length_max_15m_rank_dense_rank) / NULLIF(COUNT(CASE WHEN tag = 'VIP' THEN 1 END), 0), 2) AS tag_length_max_15m_rank_dense_rank_per_vip,
    COUNT(CASE WHEN tag = 'VIP' THEN 1 END) AS vip_count,
    ROUND(SUM(tag_length_max_15m) / NULLIF(COUNT(CASE WHEN tag = 'BUNDLE' THEN 1 END), 0), 2) AS tag_length_max_15m_per_bundle,
    ROUND(AVG(tag_length_max_15m_rank_dense_rank), 2) AS avg_tag_length_max_15m_rank_dense_rank,
    COUNT(*) FILTER (WHERE tag = 'BUNDLE') AS bundle_rows,
    MAX(tag_length_max_15m_rank_dense_rank) FILTER (WHERE tag = 'WELCOME5') AS welcome5_max_tag_length_max_15m_rank_dense_rank
FROM cleaned
GROUP BY tag
