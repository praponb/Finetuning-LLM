SELECT
    DATE_TRUNC('QUARTER', starts_on) AS quarter_start,
    SIZE(COLLECT_SET(promo_code)) AS num_promo_codes,
    COUNT_IF(channel = 'partner') AS partner_events,
    COUNT_IF(channel = 'partner') AS partner_events_2
FROM promotions
GROUP BY 1
ORDER BY partner_events_2;
