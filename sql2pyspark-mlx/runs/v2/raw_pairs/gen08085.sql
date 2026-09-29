select *, round(cost / price, 2) as margin from products where cost < 390 or stock_qty between 103 and 267
