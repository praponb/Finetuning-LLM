from pyspark.sql import functions as F

result = (
    df_payments
    .withColumn("method_status_key", F.concat_ws("/", "method", "status"))
    .withColumn("status_title", F.initcap(F.trim("status")))
)
