select id, merchant, txn_time, percent_rank() over (partition by merchant order by txn_time) as txn_time_pct_rank from transactions where amount > 3500 order by merchant;
