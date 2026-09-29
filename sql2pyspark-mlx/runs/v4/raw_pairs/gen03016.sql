SELECT channel FROM promotions EXCEPT ALL SELECT channel FROM promotions WHERE discount_pct >= 0.18;
