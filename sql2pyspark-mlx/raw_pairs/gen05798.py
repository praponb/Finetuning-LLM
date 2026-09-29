from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("quantity", "id")

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        F.when(F.col("unit_price") >= 360, "High").when(F.col("unit_price") >= 160, "Medium").otherwise("Low").alias("unit_price_tier"),
    )
)

cleaned_order_items = (
    cleaned
    .filter(F.col("unit_price") >= 190)
    .withColumn("line_total", (F.col("unit_price") - F.col("quantity")))
)

base = (
    cleaned_order_items
    .filter(F.col("quantity").between(5, 7))
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        "unit_price_tier",
        "line_total",
        F.when(F.col("unit_price") >= 410, "High").when(F.col("unit_price") >= 160, "Medium").otherwise("Low").alias("unit_price_band"),
        (F.col("quantity") - F.col("line_total")).alias("quantity_minus_line_total"),
    )
)

filtered = (
    base
    .filter(F.col("order_id").isin([1, 7, 17]))
    .select(
        "*",
        (F.col("quantity") * F.col("line_total")).alias("quantity_times_line_total"),
    )
)

ranked = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

valid = (
    ranked
    .filter(F.col("line_total") <= 51)
    .select(
        "*",
        (F.col("unit_price") - F.col("dedup_rank")).alias("unit_price_minus_dedup_rank"),
    )
)

deduped = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .alias("e")
    .join(df_orders.alias("dim"), F.col("e.order_id") == F.col("dim.id"), "inner")
    .groupBy(F.col("dim.channel").alias("channel"), F.col("dim.status").alias("status"))
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum(F.col("e.quantity_minus_line_total")), 2).alias("total_quantity_minus_line_total"),
    )
    .orderBy(F.col("status").asc_nulls_last())
)
