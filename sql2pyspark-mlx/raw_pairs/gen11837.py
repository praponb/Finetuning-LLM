from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("store_id").orderBy(F.col("units").desc_nulls_first())
w2 = Window.partitionBy("product_id")
w_part = Window.partitionBy(F.date_trunc("week", "sale_date"))
w4 = Window.orderBy("units", "id").rowsBetween(-2, -1)
w5 = Window.partitionBy("product_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("store_id").orderBy("units_share", "id")
w7 = Window.partitionBy(F.year("sale_date"))
w8 = Window.partitionBy("store_id", "product_id").orderBy(F.col("revenue_share_2").asc_nulls_first(), "id")

with_windows = (
    df_store_sales
    .withColumn("units_rank", F.rank().over(w_running))
    .withColumn("units_share", F.round((F.col("units") / F.sum("units").over(w2)), 4))
    .withColumn("revenue_share", F.round((F.col("revenue") / F.sum("revenue").over(w2)), 4))
)

scored = (
    with_windows
    .filter(F.col("units_rank") != 6)
    .withColumn("sale_date_period_count_units_rank", F.count("units_rank").over(w_part))
    .withColumn("revenue_share_avg_2_prior", F.avg("revenue_share").over(w4))
    .withColumn("last_known_units_share", F.last("units_share", ignorenulls=True).over(w5))
    .withColumn("prev_units_share", F.lag("units_share", 2).over(w6))
    .withColumn("revenue_share_2", F.round((F.col("revenue") / F.sum("revenue").over(w7)), 4))
)

base = (
    scored
    .filter(F.col("sale_date") >= F.lit("2023-07-07").cast("date"))
    .withColumn("revenue_share_2_whole", F.floor("revenue_share_2"))
)

joined = (
    base
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.price"))
)

ranked = (
    joined
    .withColumn("row_num", F.row_number().over(w8))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .groupBy("store_id")
    .agg(
        F.count("*").alias("num_rows"),
        F.percentile_approx("units_share", [0.25, 0.5, 0.75]).alias("units_share_quartiles"),
        F.sum("units_share").alias("total_units_share"),
        F.bit_or("units").alias("units_bit_or"),
    )
    .orderBy("store_id")
)
