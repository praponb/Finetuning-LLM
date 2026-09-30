WITH lines AS (
    SELECT
        id,
        customer_id,
        created_at,
        EXPLODE(coupon_codes) AS tag
    FROM carts
)
SELECT
    customer_id,
    COUNT(CASE WHEN tag = 'BUNDLE' THEN 1 END) AS bundle_count,
    SORT_ARRAY(COLLECT_SET(tag)) AS tag_list,
    SIZE(COLLECT_SET(tag)) AS num_tags
FROM lines
GROUP BY customer_id
ORDER BY bundle_count DESC;
