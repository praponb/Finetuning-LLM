WITH ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code, channel ORDER BY ends_on DESC, id) AS latest_rank
        FROM promotions
    ) x
    WHERE latest_rank = 1
),
ranked_promotions AS (
    SELECT id, promo_code
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY ends_on, id) AS rn
        FROM ranked
    ) x
    WHERE rn = 1
),
valid AS (
    SELECT *, CASE WHEN promo_code = 'SPRING10' THEN 1 ELSE 0 END AS is_spring10
    FROM ranked_promotions
    WHERE promo_code IN ('WELCOME5', 'BUNDLE', 'SPRING10')
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY is_spring10, id) AS latest_rank
    FROM valid
    QUALIFY latest_rank = 1
),
filtered AS (
    SELECT
        *,
        CASE WHEN promo_code IN ('WELCOME5', 'SPRING10') THEN 'Tier 1' ELSE 'Other' END AS promo_code_group,
        LEAST(is_spring10, latest_rank) AS least_is_spring10_latest_rank
    FROM latest
    WHERE is_spring10 <= 46
),
kept_keys AS (
    SELECT id
    FROM filtered
    EXCEPT
    SELECT id
    FROM promotions
    WHERE channel IN ('store', 'partner', 'mobile')
),
verified AS (
    SELECT s.*
    FROM filtered s
    INNER JOIN kept_keys k ON s.id = k.id
),
stats AS (
    SELECT
        CASE WHEN is_spring10 >= 87 THEN 'PREMIUM' ELSE 'STANDARD' END AS is_spring10_tier,
        CASE WHEN least_is_spring10_latest_rank >= 64 THEN 'PREMIUM' ELSE 'STANDARD' END AS least_is_spring10_latest_rank_tier,
        SORT_ARRAY(COLLECT_SET(promo_code)) AS promo_code_list,
        PERCENTILE_APPROX(is_spring10, ARRAY(0.25, 0.5, 0.75)) AS is_spring10_quartiles,
        SIZE(COLLECT_SET(promo_code)) AS num_promo_codes
    FROM verified
    GROUP BY CASE WHEN is_spring10 >= 87 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN least_is_spring10_latest_rank >= 64 THEN 'PREMIUM' ELSE 'STANDARD' END
    HAVING SIZE(COLLECT_SET(promo_code)) > 1
)
SELECT
    is_spring10_tier,
    least_is_spring10_latest_rank_tier,
    promo_code_list,
    is_spring10_quartiles,
    num_promo_codes
FROM stats
ORDER BY least_is_spring10_latest_rank_tier DESC NULLS LAST;
