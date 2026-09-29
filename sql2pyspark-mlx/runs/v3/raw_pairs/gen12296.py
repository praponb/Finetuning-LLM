from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isNotNull())
    .withColumn("method_title", F.initcap(F.trim("method")))
)
