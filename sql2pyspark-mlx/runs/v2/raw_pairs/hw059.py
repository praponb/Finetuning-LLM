from pyspark.sql import functions as F

result = df_orders.select("id", F.coalesce("discount", F.lit(0)).alias("final_discount"))
