WITH matched AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM promotions s
    LEFT JOIN promotions pr
        ON s.channel = pr.channel
       AND s.starts_on >= pr.starts_on
       AND s.starts_on <= DATE_ADD(pr.ends_on, 10)
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY promotion_discount_pct DESC NULLS LAST, id, promo_id) AS row_num
    FROM matched
    QUALIFY row_num = 1
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct AS promotion_discount_pct_2
    FROM latest src
    JOIN promotions pr
        ON src.channel <=> pr.channel
       AND src.starts_on >= pr.starts_on
       AND src.starts_on < pr.ends_on + INTERVAL 7 DAYS
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(promotion_discount_pct_2) IGNORE NULLS OVER (PARTITION BY channel ORDER BY ends_on, id, promo_id, promo_id_2 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_promotion_discount_pct_2,
        ROW_NUMBER() OVER (PARTITION BY MONTH(starts_on) ORDER BY row_num DESC, id, promo_id, promo_id_2) AS event_seq,
        LEAD(promo_id, 1, 0) OVER (PARTITION BY promotion_promo_code_2, channel ORDER BY promo_id_2, id, promo_id, promo_id_2) AS next_promo_id,
        NTILE(4) OVER (PARTITION BY promotion_promo_code ORDER BY promo_id_2, id, promo_id, promo_id_2) AS promo_id_2_bucket
    FROM attributed
),
enriched_promotions AS (
    -- Ranking and gap features
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        promo_id,
        promotion_promo_code,
        row_num,
        promo_id_2,
        last_known_promotion_discount_pct_2,
        event_seq,
        LEAD(last_known_promotion_discount_pct_2) OVER w_recent AS next_last_known_promotion_discount_pct_2,
        LEAD(promo_id) OVER w_recent AS next_promo_id_2,
        LAG(promo_id) OVER w_recent AS prev_promo_id,
        ROW_NUMBER() OVER w_recent AS event_seq_2,
        SUM(event_seq) OVER w_hist AS cumulative_event_seq
    FROM enriched
    WINDOW w_recent AS (PARTITION BY promotion_promo_code ORDER BY promotion_discount_pct, id, promo_id, promo_id_2), w_hist AS (PARTITION BY promotion_promo_code, promotion_promo_code_2 ORDER BY promo_id_2, id, promo_id, promo_id_2 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        promo_id,
        promotion_promo_code,
        row_num,
        promo_id_2,
        last_known_promotion_discount_pct_2,
        next_last_known_promotion_discount_pct_2,
        prev_promo_id,
        cumulative_event_seq,
        ROUND(event_seq / last_known_promotion_discount_pct_2, 2) AS event_seq_per_last_known_promotion_discount_pct_2
    FROM enriched_promotions
    WHERE ends_on >= DATE '2023-12-04'
      AND row_num = 1
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        ends_on,
        promo_id,
        promotion_promo_code,
        row_num,
        promo_id_2,
        next_last_known_promotion_discount_pct_2,
        prev_promo_id,
        cumulative_event_seq,
        event_seq_per_last_known_promotion_discount_pct_2,
        GREATEST(last_known_promotion_discount_pct_2, promo_id) AS greatest_last_known_promotion_discount_pct_2_promo_id
    FROM base
    WHERE starts_on > DATE '2023-07-24'
      AND next_last_known_promotion_discount_pct_2 <= 0.22
),
summary AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN cumulative_event_seq >= 42 THEN 'PREMIUM' ELSE 'STANDARD' END AS cumulative_event_seq_tier,
        DATE_TRUNC('QUARTER', ends_on) AS quarter_start,
        PERCENTILE_APPROX(event_seq_per_last_known_promotion_discount_pct_2, ARRAY(0.25, 0.5, 0.75)) AS event_seq_per_last_known_promotion_discount_pct_2_quartiles,
        COUNT(CASE WHEN promotion_promo_code = 'WELCOME5' THEN 1 END) AS welcome5_count,
        SUM(CASE WHEN promotion_promo_code = 'VIP' THEN prev_promo_id ELSE 0 END) AS vip_prev_promo_id,
        APPROX_PERCENTILE(event_seq_per_last_known_promotion_discount_pct_2, 0.5) AS event_seq_per_last_known_promotion_discount_pct_2_p50,
        PERCENTILE_APPROX(cumulative_event_seq, ARRAY(0.25, 0.5, 0.75)) AS cumulative_event_seq_quartiles,
        PERCENT_RANK() OVER (PARTITION BY CASE WHEN cumulative_event_seq >= 42 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY SUM(CASE WHEN promotion_promo_code = 'VIP' THEN prev_promo_id ELSE 0 END) DESC) AS vip_prev_promo_id_percent_rank
    FROM filtered
    GROUP BY 1, 2
    HAVING event_seq_per_last_known_promotion_discount_pct_2_p50 > 0
)
SELECT
    cumulative_event_seq_tier,
    quarter_start,
    event_seq_per_last_known_promotion_discount_pct_2_quartiles,
    welcome5_count,
    vip_prev_promo_id,
    event_seq_per_last_known_promotion_discount_pct_2_p50,
    cumulative_event_seq_quartiles
FROM summary
ORDER BY welcome5_count DESC NULLS FIRST
