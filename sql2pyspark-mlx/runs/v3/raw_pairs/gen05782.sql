select *, length(brand) brand_length
from products
where stock_qty between 272 and 370 and brand = 'Umbrella'
