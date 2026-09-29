select *, row_number() over (partition by promo_code order by discount_pct desc, id asc) as dedup_rank from promotions qualify dedup_rank = 1;
