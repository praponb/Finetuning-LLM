from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipped_date").desc(), F.col("c.id").desc())

cleaned = (
    df_orders
    .filter((F.col("order_date") > F.lit("2023-07-28").cast("date")) & (~F.col("channel").eqNullSafe(F.lit("partner"))))
    .select(
        "*",
        F.when(F.col("discount") >= 0.39, "High").when(F.col("discount") >= 0.07, "Medium").otherwise("Low").alias("discount_band"),
        F.datediff(F.current_date(), "order_date").alias("days_since_order_date"),
        F.coalesce("channel", F.lit("N/A")).alias("resolved_channel"),
    )
)

result = (
    cleaned
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.shipped_date") <= F.col("s.order_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.customer_id",
        "s.store_id",
        "s.order_date",
        "s.status",
        "s.amount",
        "s.discount",
        "s.resolved_channel",
        F.col("c.delivered_date"),
        F.col("c.shipped_date").alias("shipment_shipped_date"),
    )
)
