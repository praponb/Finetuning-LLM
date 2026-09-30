SELECT
    id,
    channel,
    promo_code,
    discount_pct,
    ROW_NUMBER() OVER (PARTITION BY channel ORDER BY ends_on, id) AS event_seq,
    LEAD(discount_pct, 1) OVER w_running AS next_discount_pct,
    STDDEV_SAMP(discount_pct) OVER w_ordered AS promo_code_stddev_samp_discount_pct
FROM promotions
WINDOW w_running AS (PARTITION BY MONTH(ends_on), channel ORDER BY ends_on, id), w_ordered AS (PARTITION BY promo_code)
