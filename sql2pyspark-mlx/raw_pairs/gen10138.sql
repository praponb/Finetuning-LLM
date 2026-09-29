select
    id,
    customer_id,
    created_at,
    coupon_codes,
    items,
    attributes,
    transform(items, i -> i.product_id) as product_ids,
    size(attributes) as num_attributes
from carts;
