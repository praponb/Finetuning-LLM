WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount_pct DESC, id) AS rank_in_group
    FROM promotions
    QUALIFY rank_in_group = 1
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, UPPER(channel) AS channel_upper
    FROM latest
    WHERE starts_on > DATE '2023-04-12'
      AND promo_code = 'VIP'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(discount_pct) OVER (ORDER BY rank_in_group, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS discount_pct_avg_3_prior,
        ROW_NUMBER() OVER (PARTITION BY promo_code, channel ORDER BY rank_in_group DESC, id) AS seq_no,
        CUME_DIST() OVER (PARTITION BY channel ORDER BY discount_pct DESC) AS discount_pct_cume_dist
    FROM cleaned
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        channel,
        starts_on,
        ends_on,
        discount_pct,
        rank_in_group,
        channel_upper,
        discount_pct_avg_3_prior,
        seq_no,
        discount_pct_cume_dist,
        FLOOR(discount_pct) AS discount_pct_whole,
        DATEDIFF(CURRENT_DATE(), ends_on) AS days_since_ends_on,
        COALESCE(channel, 'UNKNOWN') AS resolved_channel
    FROM scored
    WHERE discount_pct_avg_3_prior != 0
)
SELECT
    id,
    starts_on,
    discount_pct,
    rank_in_group,
    discount_pct_avg_3_prior,
    discount_pct_cume_dist,
    days_since_ends_on,
    resolved_channel
FROM filtered
ORDER BY starts_on DESC NULLS FIRST;
