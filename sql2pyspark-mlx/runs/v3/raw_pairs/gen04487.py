from pyspark.sql import functions as F

result = df_order_items.select("unit_price", "product_id", "order_id")
