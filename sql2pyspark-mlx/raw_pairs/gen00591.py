from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("unit_price_filled").desc())
w2 = Window.partitionBy("order_id").orderBy("unit_price_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id")
w4 = Window.orderBy("unit_price_adjusted", "id")

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & (F.col("order_id") == 9))
    .select(
        "id",
        "order_id",
        "unit_price",
        F.coalesce("unit_price", F.lit(0)).alias("unit_price_filled"),
        F.col("unit_price").cast("int").alias("unit_price_int"),
    )
)

deduped = (
    filtered
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

cleaned = (
    deduped
    .filter(F.col("order_id").isin([11, 13, 32]))
    .select(
        "id",
        "order_id",
        "unit_price",
        "unit_price_filled",
        "unit_price_int",
        F.round((F.col("unit_price") * 1.07), 2).alias("unit_price_adjusted"),
    )
)

enriched = (
    cleaned
    .select(
        "id",
        "order_id",
        "unit_price_adjusted",
        F.sum("unit_price").over(w2).alias("cumulative_unit_price"),
        F.avg("unit_price_adjusted").over(w3).alias("order_id_avg_unit_price_adjusted"),
        F.lag("unit_price_int", 2).over(w4).alias("prev_unit_price_int"),
    )
)

joined = (
    enriched
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.channel"), F.col("d.order_date"), F.col("d.amount"))
)

totals = (
    joined
    .groupBy(
        F.when(F.col("cumulative_unit_price") >= 1612, "PREMIUM").otherwise("STANDARD").alias("cumulative_unit_price_tier"),
        F.date_trunc("week", "order_date").alias("week_start"),
    )
    .agg(
        F.avg(F.when(F.col("channel") == "web", F.col("prev_unit_price_int"))).alias("web_avg_prev_unit_price_int"),
        F.countDistinct("channel").alias("distinct_channels"),
    )
)

result = (
    totals
    .select("cumulative_unit_price_tier", "week_start", "web_avg_prev_unit_price_int")
    .orderBy(F.col("week_start").desc(), "web_avg_prev_unit_price_int")
)
