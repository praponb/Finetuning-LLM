from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") != "IN_TRANSIT")
    .select(
        "order_id",
        "shipped_date",
        "status",
        F.when(F.col("status").isin(["LOST", "RETURNED"]), "Core").otherwise("Other").alias("status_group"),
        F.date_add("shipped_date", 18).alias("shipped_date_plus_18d"),
    )
)
