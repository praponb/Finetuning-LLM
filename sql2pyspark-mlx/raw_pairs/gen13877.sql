WITH lines AS (
    SELECT id, tag
    FROM carts
    LATERAL VIEW EXPLODE(coupon_codes) li AS tag
),
remaining_keys AS (
    SELECT id
    FROM lines
    INTERSECT
    SELECT id
    FROM carts
    WHERE created_at < '2025-08-01'
),
verified AS (
    SELECT s.*
    FROM lines s
    INNER JOIN remaining_keys k ON s.id = k.id
),
filtered AS (
    SELECT
        *,
        TRIM(tag) AS clean_tag,
        LENGTH(tag) AS tag_length
    FROM verified
    WHERE tag IN ('WELCOME5', 'BUNDLE', 'VIP')
),
enriched AS (
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY tag_length NULLS FIRST) AS tag_length_dense_rank,
        DENSE_RANK() OVER (PARTITION BY tag ORDER BY tag_length DESC NULLS FIRST) AS tag_length_dense_rank_2,
        AVG(tag_length) OVER (PARTITION BY tag) AS tag_avg_tag_length
    FROM filtered
),
enriched_carts AS (
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY tag ORDER BY tag_avg_tag_length NULLS FIRST) AS tag_avg_tag_length_percent_rank,
        DENSE_RANK() OVER (PARTITION BY tag ORDER BY tag_avg_tag_length NULLS LAST) AS tag_avg_tag_length_dense_rank
    FROM enriched
),
enriched_carts_2 AS (
    SELECT
        *,
        RANK() OVER (ORDER BY tag_avg_tag_length_percent_rank NULLS FIRST) AS tag_avg_tag_length_percent_rank_rank,
        SUM(tag_length_dense_rank) OVER (PARTITION BY tag) AS tag_sum_tag_length_dense_rank
    FROM enriched_carts
)
SELECT
    CASE WHEN tag_avg_tag_length_percent_rank >= 0.75 THEN 'LARGE' ELSE 'SMALL' END AS tag_avg_tag_length_percent_rank_tier,
    COUNT(*) AS cnt,
    ROUND(AVG(tag_avg_tag_length_percent_rank), 2) AS avg_tag_avg_tag_length_percent_rank,
    SORT_ARRAY(COLLECT_SET(tag)) AS tag_list,
    SUM(CASE WHEN tag = 'VIP' THEN tag_avg_tag_length ELSE 0 END) AS vip_tag_avg_tag_length
FROM enriched_carts_2
GROUP BY tag_avg_tag_length_percent_rank_tier
ORDER BY vip_tag_avg_tag_length DESC NULLS LAST;
