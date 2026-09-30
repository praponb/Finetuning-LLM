from pyspark.sql import functions as F

result = df_subscriptions.filter(~F.col("status").isin(["ACTIVE", "CANCELLED", "EXPIRED"]))
