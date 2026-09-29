from pyspark.sql import functions as F

result = (
    df_payments
    .withColumn("method_status", F.concat("method", F.lit(" "), "status"))
    .orderBy("status")
)
