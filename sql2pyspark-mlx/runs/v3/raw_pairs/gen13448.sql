select rating, country from suppliers where country <> 'DE' and (city in ('Tokyo', 'Berlin', 'Chiang Mai') or supplier_name like '%EuroGoods%') order by country
