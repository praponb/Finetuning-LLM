select id, stock_qty, ntile(2) over (order by stock_qty, id) as bucket from products where category in ('Fashion', 'Grocery') order by bucket;
