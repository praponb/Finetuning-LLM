from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("order_id").orderBy("unit_price", "id")
w3 = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc(), "id")
w4 = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc_nulls_first(), "id")
quantity_by_order_id = df_order_items.groupBy("order_id").agg(F.min("quantity").alias("min_order_item_quantity"))

valid = (
    df_order_items
    .filter(F.col("order_id").isin([3, 14, 39]))
    .select("*", F.coalesce("unit_price", F.lit(0)).alias("unit_price_filled"))
)

scored = (
    valid
    .filter(F.col("unit_price") > 330)
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("next_unit_price_filled", F.lead("unit_price_filled", 1).over(w2))
)

base = (
    scored
    .filter(F.col("product_id").isNotNull() & (F.col("product_id") == 33))
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "unit_price_filled",
        "event_seq",
        "next_unit_price_filled",
        F.round((F.col("event_seq") * 0.9), 2).alias("event_seq_scaled"),
    )
)

ranked = (
    base
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        "event_seq",
        "next_unit_price_filled",
        "event_seq_scaled",
    )
)

latest = (
    ranked
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

grouped = (
    latest
    .cube("order_id", "product_id")
    .agg(
        F.percentile_approx("event_seq", [0.25, 0.5, 0.75]).alias("event_seq_quartiles"),
        F.round(F.avg("event_seq"), 2).alias("avg_event_seq"),
        F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("avg_unit_price") < 1000000)
)

result = (
    grouped
    .join(quantity_by_order_id, on="order_id", how="left")
    .select(
        "order_id",
        "event_seq_quartiles",
        "avg_event_seq",
        "avg_unit_price",
        "grouping_level",
        F.col("min_order_item_quantity"),
    )
    .orderBy("avg_event_seq", "order_id")
)
