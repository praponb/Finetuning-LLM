select id, merchant from transactions except select id, merchant from transactions where amount < 2200;
