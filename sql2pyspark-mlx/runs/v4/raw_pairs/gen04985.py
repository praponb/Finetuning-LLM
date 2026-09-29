from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("carrier") == "Flash") & F.col("order_id").isNotNull() & (F.col("shipped_date") > "2023-04-01"))
    .select(
        "carrier",
        F.when(F.col("carrier").isin(["Kerry", "Flash"]), "Core").otherwise("Other").alias("carrier_group"),
        F.coalesce("cost", F.lit(0)).alias("cost_filled"),
    )
    .orderBy("cost_filled")
)
