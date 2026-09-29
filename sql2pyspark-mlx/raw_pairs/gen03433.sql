WITH exploded AS (
    SELECT
        base.id,
        base.customer_id,
        base.created_at,
        base.coupon_codes,
        pos + 1 AS item_no,
        item.product_id,
        item.promo
    FROM carts base
    LATERAL VIEW POSEXPLODE(base.items) x AS pos, item
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promo ORDER BY created_at, id, item_no) AS rn
        FROM exploded
    ) t
    WHERE rn = 1
),
with_prev AS (
    SELECT *, LAG(created_at) OVER (PARTITION BY promo ORDER BY created_at, id, item_no) AS prev_created_at
    FROM deduped
),
flagged AS (
    SELECT *, CASE WHEN prev_created_at IS NULL OR created_at > prev_created_at + INTERVAL '30' MINUTE THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessions_carts AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY promo ORDER BY created_at, id, item_no ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
filtered AS (
    SELECT
        id,
        customer_id,
        coupon_codes,
        item_no,
        product_id,
        promo,
        prev_created_at,
        is_new_session,
        session_id,
        DAY(created_at) created_at_day,
        FLOOR(is_new_session) is_new_session_whole,
        COALESCE(promo, 'N/A') resolved_promo
    FROM sessions_carts
    WHERE is_new_session BETWEEN 0 AND 1
),
enriched AS (
    SELECT
        e.id,
        e.customer_id,
        e.coupon_codes,
        e.item_no,
        e.is_new_session,
        e.session_id,
        e.created_at_day,
        c.credit_limit,
        c.country
    FROM filtered e
    LEFT JOIN customers c
        ON e.customer_id <=> c.id
),
base AS (
    SELECT
        id,
        customer_id,
        coupon_codes,
        item_no,
        is_new_session,
        session_id,
        created_at_day,
        credit_limit,
        country,
        CAST(created_at_day AS STRING) created_at_day_str,
        COALESCE(country, 'UNKNOWN') country_filled
    FROM enriched
    WHERE is_new_session > 0
)
SELECT
    customer_id,
    SUM(item_no) AS total_item_no,
    ROUND(AVG(credit_limit), 2) AS avg_credit_limit,
    SORT_ARRAY(COLLECT_SET(country)) AS country_list,
    SIZE(COLLECT_SET(country)) AS num_countries,
    COUNT_IF(country = 'TH') AS th_events
FROM base
GROUP BY customer_id
HAVING ROUND(AVG(credit_limit), 2) > 0
ORDER BY total_item_no DESC NULLS LAST;
