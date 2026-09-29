select
    id,
    coupon_codes,
    items,
    ship_to.country as ship_country,
    size(attributes) as num_attributes
from carts;
