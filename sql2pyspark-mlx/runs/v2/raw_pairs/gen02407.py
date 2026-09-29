from pyspark.sql import functions as F

result = (
    df_payments
    .withColumn("status_title", F.initcap(F.trim("status")))
    .withColumn("status_method_key", F.concat_ws("-", "status", "method"))
    .orderBy(F.col("status_method_key").desc())
)
