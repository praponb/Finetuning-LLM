select s.store_name, t.row_count from stores s left outer join (select store_id, count(*) as row_count from orders where shipping_fee > 11 group by store_id) t on s.id = t.store_id
