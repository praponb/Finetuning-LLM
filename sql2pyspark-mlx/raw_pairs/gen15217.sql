WITH filtered AS (
    SELECT
        id,
        channel,
        promo_code,
        ends_on,
        discount_pct,
        UPPER(promo_code) AS promo_code_upper,
        ROUND(DATEDIFF(ends_on, starts_on) / 30, 1) AS months_starts_on_to_ends_on,
        COALESCE(channel, 'UNKNOWN') AS channel_filled
    FROM promotions
    WHERE starts_on >= TO_DATE('2023-03-25')
      AND channel IS NOT NULL
)
SELECT
    id,
    promo_code,
    metric,
    val
FROM (
    SELECT
        id,
        promo_code,
        discount_pct,
        months_starts_on_to_ends_on
    FROM filtered
) src
UNPIVOT (
    val FOR metric IN (discount_pct, months_starts_on_to_ends_on)
) long_rows;
