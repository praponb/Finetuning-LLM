select id, product_id, store_id, units, round(units / sum(units) over (partition by product_id, store_id), 3) as units_share from store_sales
