WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        DATEDIFF(ends_on, starts_on) AS days_starts_on_to_ends_on
    FROM promotions
    WHERE ends_on >= DATE '2023-08-01'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(discount_pct) OVER (PARTITION BY promo_code ORDER BY discount_pct, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_discount_pct,
        AVG(discount_pct) OVER (PARTITION BY promo_code ORDER BY discount_pct, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_discount_pct,
        COUNT(discount_pct) OVER (PARTITION BY promo_code ORDER BY discount_pct, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_discount_pct,
        MIN(discount_pct) OVER (PARTITION BY promo_code ORDER BY discount_pct, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_discount_pct,
        LEAD(discount_pct, 2, 0) OVER (PARTITION BY channel ORDER BY ends_on, id) AS next_discount_pct
    FROM valid
    WHERE MONTH(ends_on) IN (3, 5, 8)
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(next_discount_pct) OVER w_ordered AS last4_count_next_discount_pct,
        AVG(next_discount_pct) OVER w_ordered AS last4_avg_next_discount_pct,
        MIN(next_discount_pct) OVER w_ordered AS last4_min_next_discount_pct
    FROM enriched
    WINDOW w_ordered AS (PARTITION BY promo_code ORDER BY last4_avg_discount_pct, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        promo_code,
        ends_on,
        discount_pct,
        last4_count_discount_pct,
        last4_min_discount_pct,
        next_discount_pct,
        last4_count_next_discount_pct,
        last4_avg_next_discount_pct
    FROM metrics
    QUALIFY RANK() OVER (PARTITION BY channel ORDER BY last4_count_discount_pct NULLS LAST) = 1
),
metrics_promotions AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(last4_count_next_discount_pct, TRUE) OVER (ORDER BY next_discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_last4_count_next_discount_pct,
        CUME_DIST() OVER (PARTITION BY MONTH(ends_on), promo_code ORDER BY last4_avg_next_discount_pct DESC) AS last4_avg_next_discount_pct_cume_dist,
        LAG(last4_min_discount_pct, 2) OVER (PARTITION BY promo_code ORDER BY last4_count_next_discount_pct, id) AS prev_last4_min_discount_pct,
        MIN(last4_min_discount_pct) OVER (PARTITION BY promo_code) AS promo_code_min_last4_min_discount_pct
    FROM deduped
)
SELECT
    id,
    promo_code,
    ends_on,
    discount_pct,
    next_discount_pct,
    last4_count_next_discount_pct,
    last4_avg_next_discount_pct,
    last4_avg_next_discount_pct_cume_dist,
    prev_last4_min_discount_pct
FROM metrics_promotions;
