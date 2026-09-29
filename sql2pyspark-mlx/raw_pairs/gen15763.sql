WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) li AS val
)
-- Ranking and gap features
SELECT
    id,
    currency,
    SUM(val) OVER w_hist AS currency_sum_val,
    SUM(val) OVER w_hist AS currency_sum_val_2,
    RANK() OVER w_recent AS val_rank,
    STDDEV_SAMP(val) OVER w_hist AS currency_stddev_samp_val,
    SUM(val) OVER w_hist AS currency_sum_val_3
FROM lines
WINDOW w_hist AS (PARTITION BY currency), w_recent AS (PARTITION BY currency ORDER BY val DESC);
