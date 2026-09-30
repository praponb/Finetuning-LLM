select * from (select channel, promo_code, discount_pct from promotions) src pivot (sum(discount_pct) as amount, count(1) as item_count for promo_code in ('SPRING10', 'VIP'))
