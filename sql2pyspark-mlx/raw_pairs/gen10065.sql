select cost, stock_qty from products where category = 'Grocery' and size(tags) >= 2 order by cost desc;
