from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier") == "Flash")
    .withColumn("status_upper", F.upper("status"))
    .orderBy("order_id", "status_upper")
)
