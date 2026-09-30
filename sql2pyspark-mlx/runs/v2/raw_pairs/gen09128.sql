select *, round(revenue, 0) as revenue_rounded, units - revenue as revenue_per_unit from store_sales where units is not null and units <= 28 order by revenue_per_unit;
