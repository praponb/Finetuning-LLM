from pyspark.sql import functions as F

result = df_order_items.select("id", "product_id", "unit_price", "order_id")
