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
        TRIM(tag) AS clean_tag,
        COALESCE(tag, 'DIRECT') AS tag_filled
    FROM lines
    WHERE created_at >= TIMESTAMP '2025-03-02 04:00:00'
      AND tag IS NOT NULL
),
base AS (
    SELECT *, LENGTH(tag) AS tag_length
    FROM valid
    WHERE created_at > TIMESTAMP '2025-03-01 17:00:00'
      AND tag IS NOT NULL
),
cleaned AS (
    SELECT
        *,
        CAST(tag_length AS DOUBLE) AS tag_length_str,
        UPPER(tag) AS tag_upper
    FROM base
    WHERE created_at > TIMESTAMP '2025-03-01 18:00:00'
),
metrics AS (
    SELECT
        id,
        tag,
        clean_tag,
        tag_filled,
        tag_length_str,
        tag_upper,
        STDDEV_SAMP(tag_length) OVER (PARTITION BY tag) AS tag_stddev_samp_tag_length,
        PERCENT_RANK() OVER (ORDER BY tag_length DESC NULLS LAST) AS tag_length_percent_rank,
        RANK() OVER (ORDER BY tag_length_str DESC NULLS LAST) AS tag_length_str_rank,
        SUM(tag_length_str) OVER (PARTITION BY tag ORDER BY CAST(created_at AS BIGINT) ASC RANGE BETWEEN 900 PRECEDING AND CURRENT ROW) AS tag_length_str_sum_15m
    FROM cleaned
),
filtered AS (
    SELECT
        *,
        ROUND(tag_length_str_sum_15m, 0) AS tag_length_str_sum_15m_rounded,
        COALESCE(tag, 'UNKNOWN') AS resolved_tag
    FROM metrics
    WHERE tag_length_str_sum_15m >= 149
),
base_carts AS (
    SELECT
        id,
        tag,
        clean_tag,
        tag_filled,
        tag_length_str,
        tag_upper,
        tag_stddev_samp_tag_length,
        tag_length_percent_rank,
        tag_length_str_rank,
        tag_length_str_sum_15m,
        tag_length_str_sum_15m_rounded,
        resolved_tag,
        LEAST(tag_length_str_rank, tag_length_str) AS least_tag_length_str_rank_tag_length_str
    FROM filtered
    WHERE tag_length_str_rank >= 7
)
SELECT
    tag,
    clean_tag,
    tag_filled,
    tag_length_str,
    tag_length_str_rank,
    tag_length_str_sum_15m,
    tag_length_str_sum_15m_rounded,
    resolved_tag,
    least_tag_length_str_rank_tag_length_str
FROM base_carts;
