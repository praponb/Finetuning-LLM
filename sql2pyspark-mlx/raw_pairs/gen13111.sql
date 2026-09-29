WITH lines AS (
    SELECT id, tag
    FROM carts
    LATERAL VIEW EXPLODE(coupon_codes) t AS tag
),
valid AS (
    SELECT
        *,
        TRIM(tag) AS clean_tag,
        COALESCE(tag, 'N/A') AS tag_filled
    FROM lines
    WHERE tag IS NOT NULL
      AND tag != 'BUNDLE'
),
cleaned AS (
    SELECT
        *,
        SUBSTRING(tag, 1, 2) AS tag_prefix,
        LOWER(tag) AS tag_lower,
        COALESCE(tag, 'OTHER') AS resolved_tag
    FROM valid
    WHERE tag = 'BUNDLE'
),
cleaned_carts AS (
    SELECT
        id,
        tag,
        clean_tag,
        tag_filled,
        tag_prefix,
        tag_lower,
        resolved_tag,
        COALESCE(tag, 'N/A') AS tag_filled_2
    FROM cleaned
    WHERE tag IN ('SPRING10', 'VIP')
),
report AS (
    SELECT
        id,
        tag,
        tag_filled,
        CASE WHEN tag RLIKE '^(WELCOME5|SPRING10)$' THEN 'CORE' ELSE 'OTHER_ZONE' END AS tag_zone
    FROM cleaned_carts
)
SELECT
    id,
    tag,
    tag_filled,
    tag_zone
FROM report
ORDER BY tag;
