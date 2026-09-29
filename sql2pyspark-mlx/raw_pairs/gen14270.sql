WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN discount_pct >= 0.36 THEN 'High' WHEN discount_pct >= 0.18 THEN 'Medium' ELSE 'Low' END discount_pct_tier
    FROM promotions
    WHERE starts_on > DATE '2023-03-22'
      AND channel IS NOT NULL
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM base s
    LEFT JOIN promotions pr
        ON COALESCE(s.channel, 'web') = pr.channel
       AND s.ends_on BETWEEN pr.starts_on AND pr.ends_on
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        promo_code,
        ends_on,
        discount_pct,
        promo_id,
        promotion_discount_pct,
        DENSE_RANK() OVER (PARTITION BY promotion_promo_code, promo_code ORDER BY promo_id NULLS FIRST) promo_id_dense_rank,
        AVG(discount_pct) OVER (PARTITION BY promo_code) promo_code_avg_discount_pct,
        AVG(discount_pct) OVER (PARTITION BY promotion_promo_code) promotion_promo_code_avg_discount_pct,
        ROUND(promo_id / SUM(promo_id) OVER (PARTITION BY promotion_promo_code), 4) promo_id_share,
        ROW_NUMBER() OVER (ORDER BY starts_on DESC, id, promo_id) event_seq
    FROM matched
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        discount_pct,
        promo_id,
        promotion_discount_pct,
        promo_code_avg_discount_pct,
        promotion_promo_code_avg_discount_pct,
        promo_id_share,
        event_seq,
        AVG(promo_id) OVER (PARTITION BY promo_code ORDER BY promotion_discount_pct, id, promo_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_avg_promo_id,
        SUM(promo_id) OVER (PARTITION BY promo_code ORDER BY promotion_discount_pct, id, promo_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_sum_promo_id,
        MIN(promo_id) OVER (PARTITION BY promo_code ORDER BY promotion_discount_pct, id, promo_id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_min_promo_id,
        SUM(promo_code_avg_discount_pct) OVER (PARTITION BY promo_code ORDER BY event_seq, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_promo_code_avg_discount_pct
    FROM enriched
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(last7_avg_promo_id) OVER (ORDER BY promo_id_share, id, promo_id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) last7_avg_promo_id_avg_3_prior,
        ROW_NUMBER() OVER (ORDER BY last7_sum_promo_id DESC, id, promo_id) seq_no,
        FIRST_VALUE(promo_code_avg_discount_pct, TRUE) OVER (ORDER BY event_seq, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_promo_code_avg_discount_pct,
        ROW_NUMBER() OVER (ORDER BY last7_sum_promo_id DESC, id, promo_id) seq_no_2
    FROM metrics
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN last7_min_promo_id >= 10.5 THEN 'High' WHEN last7_min_promo_id >= 5.3 THEN 'Medium' ELSE 'Low' END last7_min_promo_id_level
    FROM with_windows
    WHERE promo_code_avg_discount_pct >= 0
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(promo_code_avg_discount_pct) OVER (ORDER BY promo_id, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_promo_code_avg_discount_pct,
        DENSE_RANK() OVER (ORDER BY event_seq DESC NULLS LAST) event_seq_dense_rank,
        CUME_DIST() OVER (ORDER BY last7_avg_promo_id_avg_3_prior NULLS FIRST) last7_avg_promo_id_avg_3_prior_cume_dist
    FROM filtered
),
daily AS (
    -- Aggregate with conditional metrics
    SELECT
        seq_no,
        CASE WHEN running_promo_code_avg_discount_pct >= 1 THEN 'LARGE' ELSE 'SMALL' END AS running_promo_code_avg_discount_pct_tier,
        MAX(promo_id) AS max_promo_id,
        MIN(promo_code_avg_discount_pct) AS min_promo_code_avg_discount_pct,
        COUNT(*) AS row_count,
        COUNT(*) AS num_rows,
        PERCENTILE_APPROX(promotion_discount_pct, ARRAY(0.25, 0.5, 0.75)) AS promotion_discount_pct_quartiles,
        COUNT(DISTINCT id) AS distinct_ids,
        DENSE_RANK() OVER (PARTITION BY seq_no ORDER BY COUNT(*) DESC) AS row_count_dense_rank
    FROM scored
    GROUP BY seq_no, CASE WHEN running_promo_code_avg_discount_pct >= 1 THEN 'LARGE' ELSE 'SMALL' END
)
SELECT
    seq_no,
    running_promo_code_avg_discount_pct_tier,
    min_promo_code_avg_discount_pct,
    row_count,
    num_rows,
    promotion_discount_pct_quartiles,
    row_count_dense_rank
FROM daily;
