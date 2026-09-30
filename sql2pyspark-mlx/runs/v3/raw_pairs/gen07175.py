from pyspark.sql import functions as F

result = df_order_items.select("id", "order_id", "unit_price", "quantity")
