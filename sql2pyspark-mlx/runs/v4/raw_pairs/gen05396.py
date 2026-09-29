from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc(), "id")
w2 = Window.partitionBy("order_id")
w_grp = Window.partitionBy("product_id", "order_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_order_items
    .select(
        "*",
        F.ntile(10).over(w_part).alias("unit_price_bucket"),
        F.round((F.col("quantity") / F.sum("quantity").over(w2)), 4).alias("quantity_share"),
        F.sum("quantity").over(w_grp).alias("running_quantity"),
    )
)

result = (
    metrics
    .groupBy("order_id")
    .agg(
        F.percentile_approx("running_quantity", 0.9).alias("running_quantity_p90"),
        F.percentile_approx("quantity_share", 0.9).alias("quantity_share_p90"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.round(F.avg("quantity_share"), 2).alias("avg_quantity_share"),
        F.percentile_approx("unit_price", [0.25, 0.5, 0.75]).alias("unit_price_quartiles"),
        F.bit_or("quantity").alias("quantity_bit_or"),
    )
    .orderBy(F.col("avg_quantity_share").desc())
)
