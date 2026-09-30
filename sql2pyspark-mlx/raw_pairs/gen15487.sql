WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT(promo_code, '/', channel) AS promo_code_channel,
        CONCAT(promo_code, '/', channel) AS promo_code_channel_2
    FROM promotions
    WHERE channel IS NOT NULL
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(discount_pct) OVER (PARTITION BY promo_code, channel ORDER BY starts_on, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS discount_pct_avg_6_prior,
        SUM(discount_pct) OVER (PARTITION BY promo_code) AS promo_code_sum_discount_pct,
        FIRST_VALUE(channel) IGNORE NULLS OVER (PARTITION BY channel ORDER BY ends_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_channel
    FROM filtered
),
enriched_promotions AS (
    -- Running and moving aggregates
    SELECT
        id,
        promo_code,
        discount_pct,
        promo_code_channel,
        promo_code_channel_2,
        promo_code_sum_discount_pct,
        PERCENT_RANK() OVER w_part AS discount_pct_percent_rank,
        AVG(promo_code_sum_discount_pct) OVER w_ordered AS promo_code_sum_discount_pct_avg_3,
        ROW_NUMBER() OVER (ORDER BY discount_pct DESC, id) AS row_seq,
        SUM(discount_pct) OVER (PARTITION BY YEAR(starts_on), channel ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_discount_pct
    FROM enriched
    WINDOW w_part AS (PARTITION BY promo_code ORDER BY discount_pct DESC), w_ordered AS (ORDER BY promo_code_sum_discount_pct, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(row_seq, 1) OVER (ORDER BY discount_pct, id) AS prev_row_seq,
        AVG(cumulative_discount_pct) OVER (PARTITION BY promo_code ORDER BY promo_code_sum_discount_pct_avg_3, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS cumulative_discount_pct_avg_6_prior
    FROM enriched_promotions
    WHERE discount_pct_percent_rank IS NOT NULL
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        promo_code,
        promo_code_channel,
        promo_code_channel_2,
        promo_code_sum_discount_pct,
        discount_pct_percent_rank,
        promo_code_sum_discount_pct_avg_3,
        cumulative_discount_pct,
        cumulative_discount_pct_avg_6_prior,
        LAST_VALUE(discount_pct_percent_rank) IGNORE NULLS OVER (ORDER BY promo_code_sum_discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_discount_pct_percent_rank,
        ROUND(prev_row_seq / SUM(prev_row_seq) OVER (PARTITION BY promo_code), 4) AS prev_row_seq_share,
        SUM(discount_pct) OVER (PARTITION BY promo_code ORDER BY promo_code_sum_discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_discount_pct
    FROM metrics
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        promo_code,
        promo_code_channel_2,
        promo_code_sum_discount_pct,
        discount_pct_percent_rank,
        cumulative_discount_pct,
        last_known_discount_pct_percent_rank,
        prev_row_seq_share
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY running_discount_pct NULLS FIRST, id) AS rn
        FROM scored
    ) ranked
    WHERE rn = 1
),
scored_promotions AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(cumulative_discount_pct) OVER w_ordered AS running_sum_cumulative_discount_pct,
        MAX(cumulative_discount_pct) OVER w_ordered AS running_max_cumulative_discount_pct,
        AVG(cumulative_discount_pct) OVER w_ordered AS running_avg_cumulative_discount_pct
    FROM deduped
    WINDOW w_ordered AS (PARTITION BY promo_code ORDER BY discount_pct_percent_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    promo_code,
    promo_code_channel_2,
    promo_code_sum_discount_pct,
    cumulative_discount_pct,
    last_known_discount_pct_percent_rank,
    prev_row_seq_share,
    running_sum_cumulative_discount_pct,
    running_max_cumulative_discount_pct,
    CASE WHEN promo_code RLIKE '^(VIP|SPRING10)$' THEN 'GROUP_A' ELSE 'OTHER_ZONE' END AS promo_code_zone
FROM scored_promotions
ORDER BY promo_code_zone DESC NULLS FIRST, promo_code_sum_discount_pct;
