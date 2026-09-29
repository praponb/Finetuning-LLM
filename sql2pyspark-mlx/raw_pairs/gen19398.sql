WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        CASE WHEN discount_pct >= 0.34 THEN 'High' WHEN discount_pct >= 0.13 THEN 'Medium' ELSE 'Low' END AS discount_pct_band
    FROM promotions
    WHERE channel IS NOT NULL
      AND channel IS DISTINCT FROM 'partner'
),
matched_keys AS (
    -- Set intersection on keys
    SELECT promo_code, id
    FROM filtered
    INTERSECT ALL
    SELECT promo_code, id
    FROM promotions
    WHERE promo_code = 'BUNDLE'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN matched_keys k ON s.promo_code = k.promo_code AND s.id = k.id
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct_band,
        DATEDIFF(starts_on, ends_on) / 7 AS weeks_ends_on_to_starts_on,
        COALESCE(channel, 'unassigned') AS channel_filled,
        COALESCE(channel, 'UNKNOWN') AS channel_clean
    FROM reconciled
    WHERE ends_on > DATE '2023-09-17'
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, INITCAP(TRIM(channel)) AS channel_title
    FROM cleaned
    WHERE starts_on > DATE '2023-09-03'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        ends_on,
        discount_pct_band,
        channel_filled,
        channel_clean,
        channel_title,
        DENSE_RANK() OVER (ORDER BY weeks_ends_on_to_starts_on) AS weeks_ends_on_to_starts_on_dense_rank
    FROM valid
)
SELECT
    discount_pct_band,
    channel_filled,
    channel_clean,
    channel_title
FROM with_windows
