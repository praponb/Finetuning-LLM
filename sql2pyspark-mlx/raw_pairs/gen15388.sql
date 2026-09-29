WITH scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(discount_pct) OVER w_part AS prev_discount_pct,
        LEAD(discount_pct) OVER w_part AS next_discount_pct,
        ROW_NUMBER() OVER w_part AS event_seq
    FROM promotions
    WHERE promo_code IN ('WELCOME5', 'BUNDLE')
    WINDOW w_part AS (PARTITION BY channel ORDER BY starts_on, id)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        channel,
        promo_code,
        ends_on,
        discount_pct,
        next_discount_pct,
        event_seq,
        LAST_VALUE(next_discount_pct, TRUE) OVER w_grp AS last_known_next_discount_pct,
        RANK() OVER (ORDER BY prev_discount_pct DESC) AS prev_discount_pct_rank,
        FIRST_VALUE(next_discount_pct, TRUE) OVER (PARTITION BY channel ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_next_discount_pct,
        CUME_DIST() OVER (ORDER BY discount_pct) AS discount_pct_cume_dist
    FROM scored
    WINDOW w_grp AS (PARTITION BY promo_code, channel ORDER BY prev_discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
remaining_keys AS (
    -- Set difference on keys
    SELECT channel, promo_code
    FROM with_windows
    EXCEPT DISTINCT
    SELECT channel, promo_code
    FROM promotions
    WHERE YEAR(ends_on) = 2024
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN remaining_keys k ON s.channel = k.channel AND s.promo_code = k.promo_code
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        discount_pct,
        next_discount_pct,
        event_seq,
        prev_discount_pct_rank,
        SUM(first_known_next_discount_pct) OVER (PARTITION BY promo_code) AS promo_code_sum_first_known_next_discount_pct,
        CUME_DIST() OVER (ORDER BY discount_pct) AS discount_pct_cume_dist_2,
        STDDEV_SAMP(discount_pct_cume_dist) OVER (PARTITION BY YEAR(ends_on), promo_code) AS ends_on_year_stddev_samp_discount_pct_cume_dist,
        ROUND(next_discount_pct / SUM(next_discount_pct) OVER (PARTITION BY promo_code), 4) AS next_discount_pct_share,
        ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY MONTH(ends_on), promo_code), 4) AS discount_pct_share
    FROM kept_rows
),
remaining_keys_2 AS (
    -- Set intersection on keys
    SELECT id
    FROM metrics
    INTERSECT ALL
    SELECT id
    FROM promotions
    WHERE MONTH(starts_on) IN (1, 5, 8)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    INNER JOIN remaining_keys_2 k ON s.id = k.id
)
-- Grouped metrics
SELECT
    CASE WHEN ends_on_year_stddev_samp_discount_pct_cume_dist >= 1 THEN 'HIGH' ELSE 'LOW' END AS ends_on_year_stddev_samp_discount_pct_cume_dist_tier,
    CASE WHEN promo_code_sum_first_known_next_discount_pct >= 1 THEN 'LARGE' ELSE 'SMALL' END AS promo_code_sum_first_known_next_discount_pct_tier,
    CASE WHEN prev_discount_pct_rank >= 9 THEN 'LARGE' ELSE 'SMALL' END AS prev_discount_pct_rank_tier,
    PERCENTILE_APPROX(next_discount_pct, 0.5) AS next_discount_pct_p50,
    APPROX_PERCENTILE(ends_on_year_stddev_samp_discount_pct_cume_dist, ARRAY(0.25, 0.5, 0.75)) AS ends_on_year_stddev_samp_discount_pct_cume_dist_quartiles
FROM reconciled
GROUP BY CUBE (CASE WHEN ends_on_year_stddev_samp_discount_pct_cume_dist >= 1 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN promo_code_sum_first_known_next_discount_pct >= 1 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN prev_discount_pct_rank >= 9 THEN 'LARGE' ELSE 'SMALL' END);
