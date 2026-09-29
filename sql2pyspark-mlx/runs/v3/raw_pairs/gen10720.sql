select id, status, amount from (select id, status, amount, row_number() over (partition by status order by amount desc, id) as pos from payments) as t where pos = 1 order by status desc
