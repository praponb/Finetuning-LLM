select merchant, max(amount) max_amount, concat_ws(', ', sort_array(collect_set(merchant))) merchant_values
from transactions
where merchant = '7-Eleven' and (merchant is null or amount > 1500)
group by merchant
order by merchant;
