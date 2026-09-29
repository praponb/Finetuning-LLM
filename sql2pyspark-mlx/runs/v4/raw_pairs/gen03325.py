from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id")
w2 = Window.partitionBy("product_id").orderBy(F.col("revenue_filled").desc(), "id")
w3 = Window.partitionBy("product_id").orderBy("sale_date", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.orderBy(F.col("revenue_per_unit").desc(), "id")
w5 = Window.partitionBy("product_id")
w6 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("store_id").orderBy("revenue_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("region").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("cumulative_revenue")
w_part = Window.orderBy("product_id_avg_revenue_per_unit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-08-04").cast("date"))
    .withColumn("revenue_filled", F.coalesce("revenue", F.lit(0)))
    .withColumn("revenue_per_unit", (F.col("units") - F.col("revenue")))
)

metrics = (
    cleaned
    .select(
        "*",
        F.avg("revenue_per_unit").over(w).alias("product_id_avg_revenue_per_unit"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

with_dims = (
    metrics
    .alias("e")
    .join(df_stores.alias("p"), F.col("e.store_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.region"))
)

with_dims_store_sales = (
    with_dims
    .alias("src")
    .join(df_stores.alias("b"), F.col("src.store_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.opened_date"))
)

with_windows = (
    with_dims_store_sales
    .withColumn("revenue_avg_3", F.avg("revenue").over(w3))
    .withColumn("revenue_per_unit_bucket", F.ntile(4).over(w4))
)

enriched = (
    with_windows
    .select(
        "*",
        F.avg("revenue_filled").over(w5).alias("product_id_avg_revenue_filled"),
        F.sum("revenue").over(w6).alias("cumulative_revenue"),
        F.last("revenue_filled", ignorenulls=True).over(w7).alias("last_known_revenue_filled"),
    )
)

result = (
    enriched
    .select(
        "*",
        F.last("cumulative_revenue", ignorenulls=True).over(w8).alias("last_known_cumulative_revenue"),
        F.rank().over(w9).alias("cumulative_revenue_rank"),
        F.sum("units").over(w_part).alias("cumulative_units"),
    )
)
