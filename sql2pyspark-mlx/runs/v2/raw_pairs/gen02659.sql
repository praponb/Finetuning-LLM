select merchant, sort_array(collect_set(merchant)) as merchant_list from transactions group by merchant order by merchant
