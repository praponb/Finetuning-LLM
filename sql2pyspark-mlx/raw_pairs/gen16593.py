from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity").desc_nulls_last(), "id")
w_recent = Window.orderBy("quantity", "id")
w3 = Window.partitionBy("order_id").orderBy("next_quantity", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("order_id").orderBy("next_quantity", "id")
w_part = Window.orderBy("next_quantity", "id").rowsBetween(-2, -1)
revenue_by_product_id = df_store_sales.groupBy("product_id").agg(F.avg("revenue").alias("avg_store_sale_revenue"))

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .withColumn("unit_price_rounded", F.round("unit_price", 1))
)

latest = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_dims = (
    latest
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .select("s.id", "s.order_id", "s.product_id", "s.quantity", F.col("o.status"))
)

with_windows = (
    with_dims
    .filter(F.col("status").isin(["CANCELLED", "RETURNED", "SHIPPED"]))
    .select(
        "id",
        "order_id",
        "product_id",
        "status",
        F.lead("quantity", 1).over(w_recent).alias("next_quantity"),
        F.ntile(4).over(w_recent).alias("quantity_bucket"),
    )
)

metrics = (
    with_windows
    .select(
        "id",
        "order_id",
        "product_id",
        "status",
        "quantity_bucket",
        F.avg("next_quantity").over(w3).alias("next_quantity_avg_3"),
        F.lag("quantity_bucket", 2, 0).over(w4).alias("prev_quantity_bucket"),
        F.avg("next_quantity").over(w_part).alias("next_quantity_avg_2_prior"),
    )
)

enriched = (
    metrics
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .select(
        "s.*",
        F.col("o.order_date"),
        F.col("o.status").alias("order_status"),
        F.col("o.channel"),
    )
)

result = (
    enriched
    .join(revenue_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "status",
        "next_quantity_avg_3",
        "next_quantity_avg_2_prior",
        "channel",
        F.when(F.col("channel").rlike("^(partner|web)$"), "PRIORITY").when(F.col("channel").rlike("^(mobile|store)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("channel_zone"),
        F.col("avg_store_sale_revenue"),
    )
)
