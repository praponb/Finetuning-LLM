from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("unit_price", "id")
w_running = Window.partitionBy("product_id").orderBy("rn", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("product_id").orderBy(F.col("unit_price_int").desc())

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.col("unit_price").cast("int").alias("unit_price_int"),
    )
)

ranked = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

base_order_items = (
    ranked
    .filter(F.col("quantity").between(7, 8))
    .select(
        "id",
        "product_id",
        "unit_price_int",
        "rn",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
)

enriched = (
    base_order_items
    .filter(F.col("unit_price_int").between(62, 68))
    .withColumn("running_min_unit_price_int", F.min("unit_price_int").over(w_running))
    .withColumn("running_avg_unit_price_int", F.avg("unit_price_int").over(w_running))
    .withColumn("running_sum_unit_price_int", F.sum("unit_price_int").over(w_running))
    .withColumn("unit_price_int_dense_rank", F.dense_rank().over(w_grp))
)

result = (
    enriched
    .select(
        "id",
        "product_id",
        "unit_price_int",
        "running_sum_unit_price_int",
        "unit_price_int_dense_rank",
    )
    .orderBy(F.col("product_id").desc())
)
