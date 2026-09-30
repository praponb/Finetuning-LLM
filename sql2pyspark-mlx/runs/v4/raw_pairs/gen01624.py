from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["RETURNED", "LOST", "IN_TRANSIT"]))
    .select(
        "id",
        "order_id",
        F.when(F.col("carrier") == "Kerry", 1).otherwise(0).alias("is_kerry"),
        F.date_format("shipped_date", "yyyy-MM-dd").alias("shipped_date_label"),
    )
)
