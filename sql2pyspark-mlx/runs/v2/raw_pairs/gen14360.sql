with numbered as (select *, row_number() over (partition by store_id order by shipping_fee desc, id) as row_num from orders) select id, store_id, shipping_fee from numbered where row_num <= 3;
