select *
from (
    select
        id,
        category,
        cost,
        price
    from products
) src
unpivot (
    val for metric in (cost, price)
) unp;
