from pyspark.sql import functions as F

result = df_orders.filter((F.col("channel") == "store") & F.col("shipping_fee").isNotNull())
