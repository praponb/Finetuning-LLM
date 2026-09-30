select
    id,
    created_at,
    coupon_codes,
    coalesce(attributes['utm_source'], 'unknown') as utm_source_or_default,
    transform(
        filter(items, item -> item.promo is not null),
        item -> struct(item.product_id as pid, round(item.price * 0.9, 2) as discounted_price)
    ) as promo_lines
from carts
