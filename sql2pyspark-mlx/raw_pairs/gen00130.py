from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())

latest_match = (
    df_products
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.unit_price").alias("order_item_unit_price"))
)

result = (
    latest_match
    .groupBy("supplier_id")
    .agg(
        F.percentile_approx("stock_qty", 0.9).alias("stock_qty_p90"),
        F.sum(F.when(F.col("category") == "Home", F.col("stock_qty"))).alias("home_total_stock_qty"),
        F.count(F.when(F.col("category") == "Grocery", 1)).alias("grocery_rows"),
        F.percentile_approx("stock_qty", 0.9).alias("stock_qty_p90_2"),
    )
    .filter(F.col("home_total_stock_qty") > 0)
    .orderBy(F.col("supplier_id").desc())
)
