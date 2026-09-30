WITH cleaned AS (
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), ends_on) / 365.25 AS years_since_ends_on,
        DATEDIFF(CURRENT_DATE(), starts_on) / 7 AS weeks_since_starts_on,
        COALESCE(channel, 'OTHER') AS channel_clean
    FROM promotions
    WHERE YEAR(starts_on) = 2023
),
scored AS (
    SELECT
        *,
        LAG(discount_pct, 1) OVER w_recent AS prev_discount_pct,
        FIRST_VALUE(channel) IGNORE NULLS OVER w_ordered AS first_known_channel
    FROM cleaned
    WINDOW w_recent AS (ORDER BY starts_on, id), w_ordered AS (PARTITION BY promo_code ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
matched AS (
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM scored e
    LEFT JOIN promotions pr
        ON e.channel = pr.channel
       AND e.starts_on >= pr.starts_on
       AND e.starts_on <= DATE_ADD(pr.ends_on, 10)
),
enriched AS (
    SELECT
        id,
        channel,
        promo_code,
        ends_on,
        discount_pct,
        channel_clean,
        promo_id,
        promotion_discount_pct,
        AVG(years_since_ends_on) OVER (PARTITION BY promotion_promo_code ORDER BY promo_id, id, promo_id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS years_since_ends_on_avg_2_prior,
        DENSE_RANK() OVER (PARTITION BY promo_code ORDER BY promo_id NULLS FIRST) AS promo_id_dense_rank,
        MAX(discount_pct) OVER (PARTITION BY promo_code) AS promo_code_max_discount_pct
    FROM matched
    WHERE promo_code IN ('WELCOME5', 'BUNDLE', 'SPRING10')
)
SELECT
    CASE WHEN discount_pct >= 0.34 THEN 'PREMIUM' ELSE 'STANDARD' END AS discount_pct_tier,
    promo_code,
    COUNT(*) FILTER (WHERE promo_code = 'WELCOME5') AS welcome5_rows,
    COUNT_IF(promo_code = 'WELCOME5') AS welcome5_events,
    ROUND(SUM(years_since_ends_on_avg_2_prior) / NULLIF(COUNT(CASE WHEN channel = 'partner' THEN 1 END), 0), 2) AS years_since_ends_on_avg_2_prior_per_partner
FROM enriched
GROUP BY CASE WHEN discount_pct >= 0.34 THEN 'PREMIUM' ELSE 'STANDARD' END, promo_code
HAVING ROUND(SUM(years_since_ends_on_avg_2_prior) / NULLIF(COUNT(CASE WHEN channel = 'partner' THEN 1 END), 0), 2) >= 0;
