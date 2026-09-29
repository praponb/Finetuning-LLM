WITH valid AS (
    SELECT
        *,
        LENGTH(channel) AS channel_length,
        COALESCE(channel, 'UNKNOWN') AS channel_filled
    FROM promotions
    WHERE channel IS NOT NULL
),
metrics AS (
    SELECT
        id,
        promo_code,
        channel_length,
        CUME_DIST() OVER (ORDER BY channel_length DESC NULLS FIRST) AS channel_length_cume_dist,
        LAST_VALUE(channel) IGNORE NULLS OVER (ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_channel
    FROM valid
),
filtered AS (
    SELECT
        *,
        LOWER(promo_code) AS promo_code_lower,
        TRIM(promo_code) AS clean_promo_code
    FROM metrics
    WHERE channel_length_cume_dist IS NOT NULL
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY channel_length_cume_dist DESC, id) AS dedup_rank
    FROM filtered
    QUALIFY dedup_rank = 1
)
SELECT
    id,
    promo_code,
    channel_length,
    last_known_channel,
    promo_code_lower,
    clean_promo_code,
    dedup_rank
FROM ranked
ORDER BY id DESC;
