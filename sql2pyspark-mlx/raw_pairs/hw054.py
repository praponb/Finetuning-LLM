from pyspark.sql import functions as F

result = df_orders.select("id", F.nullif("discount", F.lit(0)).alias("discount_nonzero"))
