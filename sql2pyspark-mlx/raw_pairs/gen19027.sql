WITH cleaned AS (
    SELECT
        *,
        ROUND(discount_pct * 1.07, 2) AS discount_pct_adjusted,
        COALESCE(channel, 'NONE') AS resolved_channel
    FROM promotions
    WHERE starts_on >= TO_DATE('2023-04-03')
      AND channel IS DISTINCT FROM 'partner'
),
matched AS (
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM cleaned e
    LEFT JOIN promotions pr
        ON e.channel = pr.channel
       AND e.ends_on >= pr.starts_on
       AND e.ends_on < pr.ends_on + INTERVAL 14 DAYS
),
scored AS (
    SELECT
        id,
        channel,
        promo_id,
        promotion_promo_code,
        MIN(discount_pct) OVER (PARTITION BY promotion_promo_code ORDER BY starts_on, id, promo_id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_discount_pct,
        MAX(discount_pct) OVER (PARTITION BY promotion_promo_code ORDER BY starts_on, id, promo_id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_discount_pct,
        COUNT(discount_pct) OVER (PARTITION BY promotion_promo_code ORDER BY starts_on, id, promo_id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_discount_pct,
        ROW_NUMBER() OVER (PARTITION BY promotion_promo_code ORDER BY discount_pct_adjusted DESC, id, promo_id) AS event_seq
    FROM matched
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promotion_promo_code ORDER BY promo_id DESC NULLS LAST, id, promo_id) AS latest_rank
        FROM scored
    ) t
    WHERE latest_rank = 1
),
remaining_keys AS (
    SELECT id, channel
    FROM ranked
    EXCEPT ALL
    SELECT id, channel
    FROM promotions
    WHERE promo_code IN ('BUNDLE', 'SPRING10', 'WELCOME5')
),
verified AS (
    SELECT s.*
    FROM ranked s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.channel = k.channel
),
filtered AS (
    SELECT
        id,
        promo_id,
        promotion_promo_code,
        last3_min_discount_pct,
        last3_max_discount_pct,
        latest_rank,
        last3_min_discount_pct + last3_count_discount_pct AS last3_min_discount_pct_plus_last3_count_discount_pct,
        event_seq + promo_id AS event_seq_plus_promo_id,
        COALESCE(channel, 'OTHER') AS channel_clean
    FROM verified
    WHERE event_seq BETWEEN 4 AND 5
),
filtered_promotions AS (
    SELECT
        *,
        CAST(last3_max_discount_pct AS INT) AS last3_max_discount_pct_int,
        last3_min_discount_pct_plus_last3_count_discount_pct / NULLIF(event_seq_plus_promo_id, 0) AS last3_min_discount_pct_plus_last3_count_discount_pct_per_event_seq_plus_promo_id
    FROM filtered
    WHERE promotion_promo_code IS NOT NULL
      AND event_seq_plus_promo_id >= 66
),
stats AS (
    SELECT
        promo_id,
        CASE WHEN last3_min_discount_pct_plus_last3_count_discount_pct >= 82 THEN 'HIGH' ELSE 'LOW' END AS last3_min_discount_pct_plus_last3_count_discount_pct_tier,
        COUNT(*) AS event_count,
        COUNT(*) AS event_count_2,
        COUNT_IF(promotion_promo_code = 'SPRING10') AS spring10_events,
        SIZE(COLLECT_SET(promotion_promo_code)) AS num_promotion_promo_codes,
        PERCENT_RANK() OVER (PARTITION BY promo_id ORDER BY COUNT(*) DESC) AS event_count_2_percent_rank
    FROM filtered_promotions
    GROUP BY promo_id, CASE WHEN last3_min_discount_pct_plus_last3_count_discount_pct >= 82 THEN 'HIGH' ELSE 'LOW' END
)
SELECT
    promo_id,
    last3_min_discount_pct_plus_last3_count_discount_pct_tier,
    event_count,
    event_count_2,
    spring10_events,
    num_promotion_promo_codes
FROM stats
