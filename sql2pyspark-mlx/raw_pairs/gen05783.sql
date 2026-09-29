select
    id,
    customer_id,
    coupon_codes,
    ship_to,
    attributes,
    array_max(transform(items, i -> i.price)) as max_item_price,
    forall(items, i -> i.qty <= 5) as all_small_qty,
    attributes['device'] as device
from carts
