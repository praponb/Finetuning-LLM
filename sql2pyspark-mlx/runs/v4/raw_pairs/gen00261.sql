WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        CASE WHEN channel IN ('partner', 'web') THEN 'Core' ELSE 'Other' END AS channel_group,
        COALESCE(channel, 'N/A') AS channel_filled
    FROM promotions
    WHERE promo_code = 'WELCOME5'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        channel_group,
        channel_filled,
        NTILE(10) OVER w_hist AS discount_pct_bucket,
        FIRST_VALUE(channel, TRUE) OVER (ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_channel
    FROM valid
    WINDOW w_hist AS (PARTITION BY channel, promo_code ORDER BY discount_pct, id)
),
matched_keys AS (
    -- Set difference on keys
    SELECT promo_code, id
    FROM scored
    EXCEPT DISTINCT
    SELECT promo_code, id
    FROM promotions
    WHERE starts_on < '2024-12-28'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN matched_keys k ON s.promo_code = k.promo_code AND s.id = k.id
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        channel_group,
        channel_filled,
        discount_pct_bucket,
        first_known_channel,
        RANK() OVER (PARTITION BY promo_code, channel ORDER BY discount_pct_bucket DESC) AS discount_pct_bucket_rank,
        RANK() OVER (PARTITION BY promo_code ORDER BY discount_pct_bucket DESC NULLS LAST) AS discount_pct_bucket_rank_2,
        ROUND(discount_pct_bucket / SUM(discount_pct_bucket) OVER (PARTITION BY channel, promo_code), 4) AS discount_pct_bucket_share,
        ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY channel), 4) AS discount_pct_share
    FROM reconciled
),
valid_promotions AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        channel,
        starts_on,
        ends_on,
        discount_pct,
        channel_group,
        channel_filled,
        discount_pct_bucket,
        first_known_channel,
        discount_pct_bucket_rank,
        discount_pct_bucket_rank_2,
        discount_pct_bucket_share,
        discount_pct_share,
        DATEDIFF(CURRENT_DATE(), starts_on) AS days_since_starts_on
    FROM with_windows
    WHERE discount_pct_share <= 0.29
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        channel,
        starts_on,
        ends_on,
        discount_pct,
        channel_group,
        channel_filled,
        discount_pct_bucket,
        first_known_channel,
        discount_pct_bucket_rank,
        discount_pct_bucket_rank_2,
        discount_pct_bucket_share,
        discount_pct_share,
        days_since_starts_on,
        DENSE_RANK() OVER (PARTITION BY channel ORDER BY discount_pct_bucket DESC NULLS LAST) AS discount_pct_bucket_dense_rank,
        ROUND(discount_pct_bucket / SUM(discount_pct_bucket) OVER (PARTITION BY channel), 4) AS discount_pct_bucket_share_2
    FROM valid_promotions
)
-- Grouped metrics
SELECT
    channel,
    SIZE(COLLECT_SET(channel)) AS num_channels,
    AVG(discount_pct_share) AS avg_discount_pct_share,
    SUM(discount_pct) AS total_discount_pct
FROM metrics
GROUP BY channel
HAVING SIZE(COLLECT_SET(channel)) >= 1
