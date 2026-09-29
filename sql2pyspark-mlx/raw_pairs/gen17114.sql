WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, FLOOR(discount_pct) discount_pct_whole
    FROM promotions
    WHERE discount_pct >= 0.14
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY channel), 4) discount_pct_share,
        AVG(discount_pct) OVER (PARTITION BY promo_code ORDER BY starts_on, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) discount_pct_avg_2_prior,
        ROW_NUMBER() OVER (ORDER BY starts_on DESC, id) seq_no,
        LAG(discount_pct, 1) OVER (PARTITION BY MONTH(starts_on), channel ORDER BY discount_pct_whole, id) prev_discount_pct,
        SUM(discount_pct_whole) OVER (ORDER BY ends_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_discount_pct_whole
    FROM cleaned
    WHERE discount_pct_whole BETWEEN 40 AND 42
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY seq_no DESC NULLS LAST, id) AS latest_rank
    FROM scored
    QUALIFY latest_rank = 1
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATE_ADD(ends_on, 37) ends_on_plus_37d,
        FLOOR(latest_rank) latest_rank_whole,
        COALESCE(channel, 'OTHER') channel_clean
    FROM latest
    WHERE discount_pct_share < 0.73
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(discount_pct_share) OVER w_part last4_min_discount_pct_share,
        AVG(discount_pct_share) OVER w_part last4_avg_discount_pct_share,
        MAX(discount_pct_share) OVER w_part last4_max_discount_pct_share
    FROM filtered
    WINDOW w_part AS (PARTITION BY promo_code, channel ORDER BY discount_pct_whole, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    channel,
    promo_code,
    starts_on,
    ends_on,
    discount_pct,
    discount_pct_avg_2_prior,
    seq_no,
    prev_discount_pct
FROM metrics
