select
    id,
    customer_id,
    items,
    attributes,
    size(attributes) as num_attributes,
    aggregate(items, cast(0 as double), (acc, x) -> acc + x.qty * x.price, acc -> round(acc, 2)) as items_total
from carts
