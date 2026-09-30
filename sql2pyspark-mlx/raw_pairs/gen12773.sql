select
    id,
    k,
    v
from carts
lateral view explode(attributes) e as k, v;
