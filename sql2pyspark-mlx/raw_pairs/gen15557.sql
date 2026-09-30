select paid_at, ceil(amount) as amount_whole from payments where amount <= 620 order by amount_whole desc
