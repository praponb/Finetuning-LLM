WITH filtered AS (
    SELECT
        id,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        FLOOR(discount_pct) AS discount_pct_whole,
        COALESCE(channel, 'UNKNOWN') AS resolved_channel
    FROM promotions
    WHERE starts_on >= TO_DATE('2023-09-14')
      AND ends_on > '2023-06-01'
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY starts_on DESC, id) AS row_num
    FROM filtered
    QUALIFY row_num = 1
),
scored AS (
    SELECT
        id,
        starts_on,
        ends_on,
        discount_pct,
        discount_pct_whole,
        row_num,
        ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY promo_code), 4) AS discount_pct_share,
        COUNT(discount_pct_whole) OVER (PARTITION BY promo_code) AS promo_code_count_discount_pct_whole,
        SUM(discount_pct_whole) OVER (PARTITION BY promo_code ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_discount_pct_whole,
        LAST_VALUE(discount_pct_whole, TRUE) OVER (PARTITION BY promo_code ORDER BY ends_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_discount_pct_whole,
        ROW_NUMBER() OVER (ORDER BY row_num, id) AS seq_no
    FROM latest
    WHERE discount_pct_whole IS NOT NULL
),
enriched AS (
    SELECT
        *,
        MAX(last_known_discount_pct_whole) OVER (ORDER BY starts_on, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_last_known_discount_pct_whole,
        AVG(last_known_discount_pct_whole) OVER (ORDER BY starts_on, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_last_known_discount_pct_whole,
        COUNT(last_known_discount_pct_whole) OVER (ORDER BY starts_on, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_last_known_discount_pct_whole,
        NTILE(10) OVER (ORDER BY running_discount_pct_whole, id) AS running_discount_pct_whole_bucket
    FROM scored
),
with_windows AS (
    SELECT
        id,
        starts_on,
        ends_on,
        discount_pct,
        row_num,
        discount_pct_share,
        promo_code_count_discount_pct_whole,
        running_discount_pct_whole,
        last_known_discount_pct_whole,
        last3_max_last_known_discount_pct_whole,
        last3_count_last_known_discount_pct_whole,
        running_discount_pct_whole_bucket,
        SUM(discount_pct_whole) OVER (ORDER BY running_discount_pct_whole_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_discount_pct_whole,
        FIRST_VALUE(last3_avg_last_known_discount_pct_whole, TRUE) OVER (ORDER BY discount_pct_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_last3_avg_last_known_discount_pct_whole,
        SUM(last3_count_last_known_discount_pct_whole) OVER (ORDER BY last3_max_last_known_discount_pct_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_last3_count_last_known_discount_pct_whole
    FROM enriched
)
SELECT
    DATE_TRUNC('MONTH', ends_on) AS activity_month,
    MIN(last3_count_last_known_discount_pct_whole) AS min_last3_count_last_known_discount_pct_whole,
    APPROX_PERCENTILE(discount_pct_share, ARRAY(0.25, 0.5, 0.75)) AS discount_pct_share_quartiles,
    COUNT(DISTINCT id) AS distinct_ids
FROM with_windows
GROUP BY DATE_TRUNC('MONTH', ends_on)
ORDER BY distinct_ids;
