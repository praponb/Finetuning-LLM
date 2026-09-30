select
    id,
    customer_id,
    coupon_codes,
    items,
    size(filter(items, item -> item.qty >= 3)) as bulk_lines,
    try_cast(attributes['weight_kg'] as double) as weight_kg,
    coalesce(attributes['utm_source'], 'none') as utm_source_or_default
from carts;
