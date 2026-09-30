from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("line_total").desc(), "id")
w2 = Window.partitionBy("order_id")
w3 = Window.partitionBy("order_id").orderBy("line_total", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("order_id").orderBy("line_total", "id")

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("order_id").isin([18, 34]))
    .select(
        "id",
        "order_id",
        F.when(F.col("unit_price") >= 340, "High").when(F.col("unit_price") >= 160, "Medium").otherwise("Low").alias("unit_price_band"),
        (F.col("quantity") * F.col("unit_price")).alias("line_total"),
    )
)

joined = (
    base
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "inner")
    .select("a.*", F.col("b.amount"))
)

scored = (
    joined
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("order_id_stddev_samp_line_total", F.stddev_samp("line_total").over(w2))
    .withColumn("first_known_line_total", F.first("line_total", ignorenulls=True).over(w3))
    .withColumn("row_seq_2", F.row_number().over(w4))
)

result = (
    scored
    .select(
        "id",
        "line_total",
        "order_id_stddev_samp_line_total",
        "first_known_line_total",
    )
)
