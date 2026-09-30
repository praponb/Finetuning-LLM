from pyspark.sql import functions as F

result = df_shipments.filter(F.col("carrier").isin(["UPS", "Flash"]) & (F.col("cost") > 58))
