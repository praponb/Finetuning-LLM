from pyspark.sql import functions as F

filtered = (
    df_shipments
    .filter((F.col("carrier").eqNullSafe(F.lit("Kerry"))) & F.col("carrier").isNotNull())
    .withColumn("carrier_lower", F.lower("carrier"))
    .withColumn("resolved_carrier", F.coalesce("carrier", F.lit("DIRECT")))
)

result = (
    filtered
    .alias("src")
    .join(df_orders.alias("dim"), F.col("src.order_id") == F.col("dim.id"), "left")
    .groupingSets(
        [["channel", "order_status"], ["channel"], []],
        F.col("dim.channel").alias("channel"),
        F.col("dim.status").alias("order_status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("src.cost")).alias("min_cost"),
    )
    .orderBy(F.col("min_cost").asc_nulls_first())
)
