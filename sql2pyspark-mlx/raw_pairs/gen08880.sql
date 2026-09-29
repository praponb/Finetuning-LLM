SELECT channel, MIN(discount_pct) AS min_discount_pct, APPROX_PERCENTILE(discount_pct, ARRAY(0.25, 0.5, 0.75)) AS discount_pct_quartiles FROM promotions GROUP BY channel;
