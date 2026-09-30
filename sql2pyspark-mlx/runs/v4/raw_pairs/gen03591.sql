WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        SUBSTRING(promo_code, 1, 3) AS promo_code_prefix,
        COALESCE(channel, 'N/A') AS channel_filled
    FROM promotions
    WHERE ends_on > TO_DATE('2023-07-19')
      AND channel IS NOT NULL
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
        promo_code_prefix,
        channel_filled,
        COUNT(discount_pct) OVER (PARTITION BY promo_code) AS promo_code_count_discount_pct,
        ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY channel), 4) AS discount_pct_share,
        NTILE(5) OVER (ORDER BY discount_pct, id) AS discount_pct_bucket
    FROM valid
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        promo_code_prefix,
        channel_filled,
        promo_code_count_discount_pct,
        discount_pct_share,
        discount_pct_bucket,
        ROUND(discount_pct_bucket / SUM(discount_pct_bucket) OVER (PARTITION BY promo_code), 4) AS discount_pct_bucket_share,
        NTILE(4) OVER (ORDER BY discount_pct, id) AS discount_pct_bucket_2,
        LAG(discount_pct_bucket, 1) OVER w_part AS prev_discount_pct_bucket
    FROM with_windows
    WINDOW w_part AS (PARTITION BY promo_code ORDER BY ends_on, id)
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        promo_code_count_discount_pct,
        discount_pct_share,
        discount_pct_bucket,
        discount_pct_bucket_share,
        discount_pct_bucket_2,
        prev_discount_pct_bucket,
        discount_pct_share * discount_pct_bucket_2 AS discount_pct_share_times_discount_pct_bucket_2
    FROM scored
    WHERE discount_pct > 0.26
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id, channel
    FROM cleaned
    INTERSECT ALL
    SELECT id, channel
    FROM promotions
    WHERE channel = 'partner'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN kept_keys k ON s.id = k.id AND s.channel = k.channel
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, MONTH(starts_on) AS starts_on_month
    FROM reconciled
    WHERE discount_pct_share > 0.74
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        channel,
        promo_code,
        discount_pct_bucket_share
    FROM base
) src
PIVOT (
    AVG(discount_pct_bucket_share) AS amount
    FOR promo_code IN ('VIP', 'BUNDLE')
);
