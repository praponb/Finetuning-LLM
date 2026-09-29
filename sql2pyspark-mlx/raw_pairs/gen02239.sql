WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        promo_code,
        starts_on,
        ends_on
    FROM promotions
    QUALIFY RANK() OVER (PARTITION BY channel ORDER BY discount_pct) = 1
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, DATEDIFF(starts_on, ends_on) / 7 AS weeks_ends_on_to_starts_on
    FROM ranked
    WHERE ends_on <= '2025-05-01'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(weeks_ends_on_to_starts_on) OVER (PARTITION BY promo_code ORDER BY ends_on, id) AS next_weeks_ends_on_to_starts_on,
        ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY ends_on, id) AS seq_no,
        LAST_VALUE(weeks_ends_on_to_starts_on, TRUE) OVER (PARTITION BY MONTH(starts_on), promo_code ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_weeks_ends_on_to_starts_on
    FROM base
    WHERE promo_code IN ('WELCOME5', 'BUNDLE', 'VIP')
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        weeks_ends_on_to_starts_on,
        next_weeks_ends_on_to_starts_on,
        seq_no,
        last_known_weeks_ends_on_to_starts_on
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY ends_on DESC, id) = 1
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        weeks_ends_on_to_starts_on,
        next_weeks_ends_on_to_starts_on,
        seq_no,
        last_known_weeks_ends_on_to_starts_on,
        ROUND(seq_no * 1.07, 2) AS seq_no_scaled
    FROM latest
    WHERE weeks_ends_on_to_starts_on >= 20
),
enriched_promotions AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(3) OVER w_running AS seq_no_scaled_bucket,
        SUM(weeks_ends_on_to_starts_on) OVER w_hist AS cumulative_weeks_ends_on_to_starts_on,
        FIRST_VALUE(last_known_weeks_ends_on_to_starts_on) IGNORE NULLS OVER w_hist AS first_known_last_known_weeks_ends_on_to_starts_on,
        SUM(next_weeks_ends_on_to_starts_on) OVER (ORDER BY last_known_weeks_ends_on_to_starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_next_weeks_ends_on_to_starts_on,
        SUM(weeks_ends_on_to_starts_on) OVER (ORDER BY next_weeks_ends_on_to_starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_weeks_ends_on_to_starts_on
    FROM filtered
    WINDOW w_running AS (ORDER BY seq_no_scaled, id), w_hist AS (ORDER BY weeks_ends_on_to_starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Multi-level aggregation
SELECT
    seq_no,
    CASE WHEN cumulative_next_weeks_ends_on_to_starts_on >= 397 THEN 'HIGH' ELSE 'LOW' END AS cumulative_next_weeks_ends_on_to_starts_on_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENTILE_APPROX(cumulative_next_weeks_ends_on_to_starts_on, ARRAY(0.25, 0.5, 0.75)) AS cumulative_next_weeks_ends_on_to_starts_on_quartiles,
    ROUND(AVG(seq_no_scaled), 2) AS avg_seq_no_scaled
FROM enriched_promotions
GROUP BY seq_no, CASE WHEN cumulative_next_weeks_ends_on_to_starts_on >= 397 THEN 'HIGH' ELSE 'LOW' END;
