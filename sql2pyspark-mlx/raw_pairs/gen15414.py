from pyspark.sql import functions as F

valid = (
    df_shipments
    .filter((F.col("delivered_date") > F.lit("2023-10-02").cast("date")) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.trim("carrier").alias("clean_carrier"),
        F.coalesce("carrier", F.lit("NONE")).alias("carrier_clean"),
    )
)

result = (
    valid
    .groupBy("order_id")
    .pivot("status", ["LOST", "RETURNED", "DELIVERED"])
    .agg(F.sum("cost"))
    .select(
        "order_id",
        F.col("LOST").alias("lost"),
        F.col("RETURNED").alias("returned"),
        F.col("DELIVERED").alias("delivered"),
    )
)
