WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CEIL(discount_pct) AS discount_pct_whole,
        DATEDIFF(starts_on, ends_on) AS days_ends_on_to_starts_on
    FROM promotions
    WHERE channel IS NOT NULL
      AND channel IS NOT DISTINCT FROM 'partner'
)
-- Deduplicate with ROW_NUMBER
SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code, channel ORDER BY discount_pct DESC, id) AS rank_in_group
    FROM cleaned
) t
WHERE rank_in_group = 1;
