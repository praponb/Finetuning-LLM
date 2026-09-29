select id, segment, name, credit_limit, cast(is_active as int) is_active_flag from customers where name is not null order by id;
