from pyspark.sql import functions as F

result = df_order_items.select("id", "unit_price", "quantity", "product_id")
