WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        channel,
        starts_on,
        ends_on,
        discount_pct,
        CONCAT_WS('-', channel, promo_code) AS channel_promo_code_key
    FROM promotions
    WHERE ends_on > DATE '2023-11-21'
      AND channel IS NOT NULL
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        channel,
        discount_pct,
        channel_promo_code_key,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY starts_on, id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY ends_on, id) AS seq_no_2,
        PERCENT_RANK() OVER w_running AS discount_pct_percent_rank
    FROM filtered
    WINDOW w_running AS (PARTITION BY channel ORDER BY discount_pct)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        discount_pct,
        channel_promo_code_key,
        seq_no,
        discount_pct_percent_rank,
        LAG(seq_no, 1, 0) OVER (PARTITION BY channel ORDER BY discount_pct, id) AS prev_seq_no,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY seq_no_2 DESC, id) AS row_seq,
        NTILE(4) OVER (ORDER BY seq_no, id) AS seq_no_bucket,
        NTILE(3) OVER (ORDER BY discount_pct DESC, id) AS discount_pct_bucket,
        ROUND(seq_no / SUM(seq_no) OVER (PARTITION BY channel), 4) AS seq_no_share
    FROM metrics
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        prev_seq_no,
        discount_pct_bucket,
        seq_no_share,
        SUM(prev_seq_no) OVER w_recent AS running_sum_prev_seq_no,
        MAX(prev_seq_no) OVER w_recent AS running_max_prev_seq_no,
        AVG(prev_seq_no) OVER w_recent AS running_avg_prev_seq_no
    FROM scored
    WHERE discount_pct_bucket >= 5
    WINDOW w_recent AS (ORDER BY discount_pct_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(running_avg_prev_seq_no) OVER (ORDER BY prev_seq_no, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS running_avg_prev_seq_no_avg_3_prior,
        NTILE(3) OVER w_running AS discount_pct_bucket_bucket,
        LAG(prev_seq_no, 1) OVER w_running AS prev_prev_seq_no
    FROM with_windows
    WINDOW w_running AS (ORDER BY discount_pct_bucket, id)
),
enriched_promotions AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        running_sum_prev_seq_no,
        running_max_prev_seq_no,
        running_avg_prev_seq_no,
        discount_pct_bucket_bucket,
        prev_prev_seq_no,
        FIRST_VALUE(running_sum_prev_seq_no) IGNORE NULLS OVER (ORDER BY running_sum_prev_seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_sum_prev_seq_no,
        ROW_NUMBER() OVER (ORDER BY prev_seq_no DESC, id) AS event_seq
    FROM enriched
)
-- Grouped metrics
SELECT
    CASE WHEN discount_pct_bucket_bucket >= 8 THEN 'HIGH' ELSE 'LOW' END AS discount_pct_bucket_bucket_tier,
    ROUND(AVG(discount_pct_bucket_bucket), 2) AS avg_discount_pct_bucket_bucket,
    PERCENTILE_APPROX(first_known_running_sum_prev_seq_no, 0.5) AS first_known_running_sum_prev_seq_no_p50,
    PERCENTILE_APPROX(event_seq, ARRAY(0.25, 0.5, 0.75)) AS event_seq_quartiles
FROM enriched_promotions
GROUP BY discount_pct_bucket_bucket_tier
HAVING PERCENTILE_APPROX(first_known_running_sum_prev_seq_no, 0.5) >= 0
ORDER BY avg_discount_pct_bucket_bucket DESC NULLS FIRST
