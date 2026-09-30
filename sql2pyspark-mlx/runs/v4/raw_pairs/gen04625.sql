select
    id,
    coupon_codes,
    items,
    ship_to,
    attributes,
    array_max(transform(items, item -> item.price)) as max_item_price,
    coalesce(attributes['device'], 'unknown') as device_or_default,
    size(filter(items, item -> item.qty >= 3)) as bulk_lines
from carts
where array_contains(coupon_codes, 'VIP');
