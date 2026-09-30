WITH filtered AS (
    SELECT *, CONCAT(channel, ', ', promo_code) AS channel_promo_code
    FROM promotions
    WHERE ends_on > TO_DATE('2023-06-16')
      AND channel = 'store'
)
SELECT
    s.*,
    pr.id AS promo_id,
    pr.promo_code AS promotion_promo_code,
    pr.discount_pct AS promotion_discount_pct
FROM filtered s
LEFT JOIN promotions pr
    ON s.channel = pr.channel
   AND s.ends_on >= pr.starts_on
   AND s.ends_on <= DATE_ADD(pr.ends_on, 7);
