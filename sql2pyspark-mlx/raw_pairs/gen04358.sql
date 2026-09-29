WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        TRIM(channel) AS clean_channel,
        CAST(discount_pct AS INT) AS discount_pct_int
    FROM promotions
    WHERE starts_on >= DATE '2023-07-11'
      AND channel IS NOT NULL
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY channel ORDER BY discount_pct DESC) AS dedup_rank
    FROM filtered
    QUALIFY dedup_rank = 1
),
filtered_promotions AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        dedup_rank / NULLIF(discount_pct_int, 0) AS dedup_rank_per_discount_pct_int,
        INITCAP(TRIM(promo_code)) AS promo_code_title
    FROM latest
    WHERE ends_on >= TO_DATE('2023-11-11')
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        INITCAP(TRIM(channel)) AS channel_title,
        COALESCE(channel, 'NONE') AS resolved_channel
    FROM filtered_promotions
    WHERE ends_on > DATE '2023-10-23'
      AND channel IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        ends_on,
        clean_channel,
        dedup_rank,
        dedup_rank_per_discount_pct_int,
        promo_code_title,
        channel_title,
        resolved_channel,
        SUM(discount_pct_int) OVER w_recent AS last4_sum_discount_pct_int,
        AVG(discount_pct_int) OVER w_recent AS last4_avg_discount_pct_int,
        MAX(discount_pct_int) OVER w_recent AS last4_max_discount_pct_int,
        ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY discount_pct DESC, id) AS event_seq
    FROM base
    WINDOW w_recent AS (PARTITION BY channel, promo_code ORDER BY starts_on, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    ends_on,
    clean_channel,
    dedup_rank,
    dedup_rank_per_discount_pct_int,
    promo_code_title,
    last4_avg_discount_pct_int,
    last4_max_discount_pct_int,
    event_seq
FROM metrics;
