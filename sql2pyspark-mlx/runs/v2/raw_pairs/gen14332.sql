with RankedTransactions as (select *, row_number() over (partition by merchant order by amount desc, id) as pos from transactions) select * from RankedTransactions where pos <= 3
