from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(-5, Window.currentRow)
w_running = Window.partitionBy("order_id", "product_id")
w_recent = Window.orderBy("unit_price_avg_5", "id")
w5 = Window.partitionBy("order_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("product_id").orderBy("unit_price_avg_5", "id").rowsBetween(-2, -1)
w7 = Window.partitionBy("order_id").orderBy(F.col("unit_price_avg_5_avg_2_prior").desc(), "id")
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.max("cost").alias("max_shipment_cost"))

scored = (
    df_order_items
    .filter(F.col("unit_price") <= 130)
    .withColumn("cumulative_quantity", F.sum("quantity").over(w_grp))
    .withColumn("unit_price_avg_5", F.avg("unit_price").over(w2))
    .withColumn("corr_quantity_unit_price", F.corr("quantity", "unit_price").over(w_running))
)

with_windows = (
    scored
    .withColumn("unit_price_avg_5_bucket", F.ntile(10).over(w_recent))
    .withColumn("running_unit_price", F.sum("unit_price").over(w5))
    .withColumn("unit_price_avg_5_avg_2_prior", F.avg("unit_price_avg_5").over(w_ordered))
)

joined = (
    with_windows
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.product_name"))
)

ranked = (
    joined
    .withColumn("rank_in_group", F.row_number().over(w7))
    .filter(F.col("rank_in_group") == 1)
)

matched_keys = (
    ranked
    .select("order_id")
    .exceptAll(
        df_order_items
        .filter(F.col("product_id").isNotNull())
        .select("order_id")
    )
)

kept_rows = (
    ranked
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        "cumulative_quantity",
        "unit_price_avg_5",
        "corr_quantity_unit_price",
        "running_unit_price",
        "unit_price_avg_5_avg_2_prior",
        F.col("max_shipment_cost"),
    )
)
