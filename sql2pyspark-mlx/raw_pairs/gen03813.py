from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("cost", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("status").orderBy(F.col("shipped_date_period").cast("bigint")).rangeBetween(-2592000, Window.currentRow)

filtered = (
    df_shipments
    .filter(F.year("delivered_date") == 2024)
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "cost",
        "status",
        F.date_trunc("week", "shipped_date").alias("shipped_date_period"),
        F.coalesce("carrier", F.lit("OTHER")).alias("carrier_clean"),
    )
)

scored = (
    filtered
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "status",
        "shipped_date_period",
        "carrier_clean",
        F.avg("cost").over(w).alias("cost_avg_6_prior"),
        F.count("cost").over(w2).alias("cost_count_30d"),
    )
)

with_dims = (
    scored
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "inner")
    .filter(F.col("d.store_id").isNull())
    .select(
        "src.id",
        "src.order_id",
        "src.delivered_date",
        "src.cost_avg_6_prior",
        F.col("d.order_date"),
        F.col("d.channel"),
    )
)

cleaned = (
    with_dims
    .filter(F.col("delivered_date") >= F.lit("2023-09-09").cast("date"))
    .select(
        "id",
        "order_id",
        "order_date",
        F.when(F.col("channel") == "partner", 1).otherwise(0).alias("is_partner"),
    )
)

result = (
    cleaned
    .alias("a")
    .join(df_orders.alias("dim"), F.col("a.order_id") == F.col("dim.id"), "left")
    .groupBy(F.col("dim.status").alias("status"))
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("a.is_partner")).alias("max_is_partner"),
    )
)
