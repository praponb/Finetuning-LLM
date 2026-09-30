from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("month", "sale_date"))
w2 = Window.partitionBy("store_id").orderBy(F.col("units").asc_nulls_last())
w3 = Window.partitionBy("product_id").orderBy("units_percent_rank", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy(F.month("launch_date"), "product_id").orderBy("launch_date", "id")
w5 = Window.partitionBy("product_id").orderBy(F.col("launch_date").desc(), "id")
w6 = Window.partitionBy(F.year("launch_date"), "product_id").orderBy("units_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("units_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("product_id").orderBy(F.col("units_percent_rank_avg_6").desc(), "id")

scored = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "revenue",
        F.round((F.col("revenue") / F.sum("revenue").over(w)), 4).alias("revenue_share"),
        F.percent_rank().over(w2).alias("units_percent_rank"),
    )
)

cleaned = (
    scored
    .filter(F.col("sale_date") >= F.to_date(F.lit("2023-05-12")))
    .withColumn("revenue_whole", F.ceil("revenue"))
)

enriched = (
    cleaned
    .alias("s")
    .join(df_products.alias("b"), F.col("s.product_id") == F.col("b.id"), "left")
    .filter(F.col("b.cost").between(190, 490))
    .select(
        "s.id",
        "s.store_id",
        "s.product_id",
        "s.units_percent_rank",
        F.col("b.launch_date"),
    )
)

with_windows = (
    enriched
    .withColumn("units_percent_rank_avg_6", F.avg("units_percent_rank").over(w3))
    .withColumn("prev_units_percent_rank", F.lag("units_percent_rank", 2).over(w4))
    .withColumn("row_seq", F.row_number().over(w5))
    .withColumn("running_units_percent_rank", F.sum("units_percent_rank").over(w6))
    .withColumn("cumulative_units_percent_rank", F.sum("units_percent_rank").over(w7))
)

latest = (
    with_windows
    .withColumn("row_num", F.row_number().over(w8))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .rollup(F.col("p.category").alias("category"))
    .agg(
        F.count("*").alias("rows_total"),
        F.avg(F.col("a.prev_units_percent_rank")).alias("avg_prev_units_percent_rank"),
    )
    .orderBy(F.col("category").desc())
)
