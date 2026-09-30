WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        channel,
        promo_code,
        ends_on,
        discount_pct,
        CONCAT_WS('/', channel, promo_code) AS channel_promo_code_key,
        CEIL(discount_pct) AS discount_pct_whole
    FROM promotions
    WHERE ends_on > DATE '2023-10-25'
),
cleaned_promotions AS (
    -- Keep recent, valid rows
    SELECT *, CAST(discount_pct AS INT) AS discount_pct_int
    FROM cleaned
    WHERE ends_on >= DATE '2023-10-12'
      AND channel IS NOT NULL
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM cleaned_promotions a
    INNER JOIN promotions pr
        ON a.channel <=> pr.channel
       AND a.ends_on >= pr.starts_on
       AND a.ends_on <= DATE_ADD(pr.ends_on, 7)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        channel,
        discount_pct_whole,
        discount_pct_int,
        promo_id,
        LEAD(promotion_discount_pct) OVER w_recent AS next_promotion_discount_pct,
        LAG(promotion_discount_pct) OVER w_recent AS prev_promotion_discount_pct,
        LEAD(discount_pct) OVER w_recent AS next_discount_pct,
        ROW_NUMBER() OVER w_recent AS seq_no,
        MIN(discount_pct_int) OVER (PARTITION BY channel) AS channel_min_discount_pct_int
    FROM attributed
    WINDOW w_recent AS (PARTITION BY channel ORDER BY discount_pct_int, id, promo_id)
),
scored_promotions AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(prev_promotion_discount_pct) OVER (PARTITION BY channel ORDER BY seq_no, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_prev_promotion_discount_pct,
        LAG(discount_pct_whole, 1) OVER (PARTITION BY channel ORDER BY next_promotion_discount_pct, id, promo_id) AS prev_discount_pct_whole,
        LEAD(discount_pct_whole, 2) OVER (PARTITION BY channel ORDER BY seq_no, id, promo_id) AS next_discount_pct_whole,
        MIN(seq_no) OVER (PARTITION BY channel) AS channel_min_seq_no,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount_pct_whole DESC, id, promo_id) AS row_seq
    FROM scored
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(next_discount_pct_whole) OVER (PARTITION BY channel ORDER BY next_promotion_discount_pct, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_next_discount_pct_whole,
        DENSE_RANK() OVER (PARTITION BY channel ORDER BY next_discount_pct_whole DESC NULLS LAST) AS next_discount_pct_whole_dense_rank,
        MIN(channel_min_discount_pct_int) OVER w_hist AS channel_min_channel_min_discount_pct_int,
        COUNT(next_discount_pct) OVER w_hist AS channel_count_next_discount_pct,
        ROUND(promo_id / SUM(promo_id) OVER (PARTITION BY channel), 4) AS promo_id_share
    FROM scored_promotions
    WINDOW w_hist AS (PARTITION BY channel)
)
-- Multi-level aggregation
SELECT
    CASE WHEN discount_pct_whole >= 67 THEN 'LARGE' ELSE 'SMALL' END AS discount_pct_whole_tier,
    promo_id,
    channel,
    SORT_ARRAY(COLLECT_SET(channel)) AS channel_list,
    ROUND(SUM(promo_id) / NULLIF(COUNT(CASE WHEN channel = 'mobile' THEN 1 END), 0), 2) AS promo_id_per_mobile,
    APPROX_PERCENTILE(prev_promotion_discount_pct, 0.9) AS prev_promotion_discount_pct_p90,
    COUNT(*) AS row_count,
    COUNT_IF(channel = 'partner') AS partner_events,
    ROUND(AVG(prev_discount_pct_whole), 2) AS avg_prev_discount_pct_whole
FROM enriched
GROUP BY 1, 2, 3
HAVING COUNT_IF(channel = 'partner') > 1
