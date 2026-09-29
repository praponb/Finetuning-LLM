select id, quantity, percent_rank() over (order by quantity desc) quantity_pct_rank from order_items where quantity < 7
