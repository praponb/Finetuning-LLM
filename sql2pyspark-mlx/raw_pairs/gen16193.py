from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

with_last = (
    df_products
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.unit_price").alias("order_item_unit_price"))
)

result = (
    with_last
    .groupBy("brand")
    .agg(
        F.stddev_samp("cost").alias("cost_stddev"),
        F.bit_or("stock_qty").alias("stock_qty_bit_or"),
        F.max(F.when(F.col("brand") == "Globex", F.col("stock_qty"))).alias("globex_max_stock_qty"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("stock_qty_bit_or").desc())
)
