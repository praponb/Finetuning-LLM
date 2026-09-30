select
    id,
    items,
    ship_to,
    attributes,
    attributes['gift'] as gift,
    coalesce(attributes['gift'], 'none') as gift_or_default,
    attributes['device'] as device,
    ship_to.country as ship_country,
    size(attributes) as num_attributes
from carts
