from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("status") == "LOST") & (F.col("status") == "DELIVERED") & (F.col("cost") <= 77))
    .withColumn("status_length", F.length("status"))
    .withColumn("delivered_date_weekofyear", F.weekofyear("delivered_date"))
)
