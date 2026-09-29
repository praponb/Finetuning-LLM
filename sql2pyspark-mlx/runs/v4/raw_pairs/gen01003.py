from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("order_id", "status")
    .pivot("carrier", ["Kerry", "DHL"])
    .agg(F.max("cost").alias("value_sum"), F.count(F.lit(1)).alias("n"))
    .select(
        "order_id",
        "status",
        F.col("Kerry_value_sum").alias("kerry_value_sum"),
        F.col("Kerry_n").alias("kerry_n"),
        F.col("DHL_value_sum").alias("dhl_value_sum"),
        F.col("DHL_n").alias("dhl_n"),
    )
)
