from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("delivered_date").desc_nulls_first(), "id")

cleaned = (
    df_shipments
    .filter((F.col("delivered_date") >= F.lit("2023-03-12").cast("date")) & (F.col("status") == "IN_TRANSIT"))
    .withColumn("status_title", F.initcap(F.trim("status")))
    .withColumn("carrier_status_key", F.concat_ws(",", "carrier", "status"))
)

valid = (
    cleaned
    .filter((F.col("shipped_date") >= F.to_date(F.lit("2023-06-28"))) & F.col("order_id").isNotNull())
    .select(
        "*",
        (F.datediff("delivered_date", "shipped_date") / 7).alias("weeks_shipped_date_to_delivered_date"),
        F.coalesce("carrier", F.lit("N/A")).alias("carrier_clean"),
    )
)

ranked = (
    valid
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "status",
        "weeks_shipped_date_to_delivered_date",
        "carrier_clean",
    )
)

joined = (
    ranked
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "inner")
    .select(
        "s.id",
        "s.order_id",
        "s.delivered_date",
        "s.status",
        "s.weeks_shipped_date_to_delivered_date",
        F.col("p.order_date"),
    )
)

pivoted = (
    joined
    .groupBy("order_id")
    .pivot("status", ["DELIVERED", "RETURNED", "IN_TRANSIT"])
    .agg(F.max("weeks_shipped_date_to_delivered_date").alias("total"))
    .select(
        "order_id",
        F.col("DELIVERED").alias("delivered"),
        F.col("RETURNED").alias("returned"),
        F.col("IN_TRANSIT").alias("in_transit"),
    )
)

result = (
    pivoted
    .select("order_id", "delivered", "returned")
    .orderBy(F.col("returned").asc_nulls_first())
)
