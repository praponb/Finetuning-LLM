WITH filtered AS (
    SELECT *, FLOOR(discount_pct) AS discount_pct_whole
    FROM promotions
    WHERE starts_on >= DATE '2023-10-09'
),
matched AS (
    SELECT
        a.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM filtered a
    LEFT JOIN promotions pr
        ON COALESCE(a.channel, 'web') = pr.channel
       AND a.starts_on >= pr.starts_on
       AND a.starts_on < pr.ends_on + INTERVAL '30' DAY
),
latest AS (
    SELECT
        id,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        promo_id,
        promotion_discount_pct
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promotion_promo_code, promo_code ORDER BY promotion_discount_pct, id, promo_id) AS rn
        FROM matched
    ) t
    WHERE rn = 1
),
metrics AS (
    SELECT
        *,
        MIN(promo_id) OVER (PARTITION BY promo_code ORDER BY ends_on, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_promo_id,
        COUNT(promo_id) OVER (PARTITION BY promo_code ORDER BY ends_on, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_promo_id,
        SUM(promo_id) OVER (PARTITION BY promo_code ORDER BY ends_on, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_promo_id,
        ROW_NUMBER() OVER (PARTITION BY YEAR(starts_on), promo_code ORDER BY promo_id DESC, id, promo_id) AS row_seq
    FROM latest
),
with_windows AS (
    SELECT
        id,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        promo_id,
        running_min_promo_id,
        running_count_promo_id,
        running_sum_promo_id,
        SUM(running_count_promo_id) OVER w_running AS cumulative_running_count_promo_id,
        AVG(discount_pct) OVER w_running_2 AS discount_pct_avg_5_prior,
        ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY row_seq, id, promo_id) AS seq_no
    FROM metrics
    WINDOW w_running AS (PARTITION BY promo_code ORDER BY row_seq, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running_2 AS (PARTITION BY promo_code ORDER BY starts_on, id, promo_id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING)
)
SELECT
    promo_id,
    CASE WHEN running_sum_promo_id >= 37 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_sum_promo_id_tier,
    SORT_ARRAY(COLLECT_SET(promo_code)) AS promo_code_list,
    SUM(discount_pct_avg_5_prior) FILTER (WHERE promo_code = 'BUNDLE') AS bundle_total_discount_pct_avg_5_prior,
    APPROX_PERCENTILE(discount_pct_avg_5_prior, ARRAY(0.25, 0.5, 0.75)) AS discount_pct_avg_5_prior_quartiles,
    COUNT(CASE WHEN promo_code = 'VIP' THEN 1 END) AS vip_count,
    ROUND(SUM(seq_no) / NULLIF(COUNT(CASE WHEN promo_code = 'VIP' THEN 1 END), 0), 2) AS seq_no_per_vip,
    SUM(CASE WHEN promo_code = 'WELCOME5' THEN running_count_promo_id ELSE 0 END) AS welcome5_running_count_promo_id,
    DENSE_RANK() OVER (PARTITION BY promo_id ORDER BY SUM(CASE WHEN promo_code = 'WELCOME5' THEN running_count_promo_id ELSE 0 END) DESC) AS welcome5_running_count_promo_id_dense_rank
FROM with_windows
GROUP BY promo_id, CASE WHEN running_sum_promo_id >= 37 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY bundle_total_discount_pct_avg_5_prior NULLS FIRST;
