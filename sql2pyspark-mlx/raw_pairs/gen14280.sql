select sort_array(collect_set(status)) as status_list, max(amount) as max_amount from payments where status in ('SUCCESS', 'REFUNDED', 'FAILED') or status in ('SUCCESS', 'REFUNDED', 'FAILED')
