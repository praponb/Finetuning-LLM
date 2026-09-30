from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("units_whole").asc_nulls_first(), "id")
w2 = Window.partitionBy("product_id").orderBy("revenue", "id")
w3 = Window.orderBy(F.col("revenue").desc_nulls_first())
w_running = Window.partitionBy("store_id").orderBy(F.col("revenue").desc(), "id")
w5 = Window.partitionBy("store_id", "product_id").orderBy("rn", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("store_id").orderBy("units_whole", "id").rowsBetween(-6, Window.currentRow)
w7 = Window.orderBy(F.col("revenue_percent_rank").desc(), "id")
w8 = Window.partitionBy("store_id")

base = (
    df_store_sales
    .filter(F.col("units") != 17)
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "revenue",
        F.ceil("units").alias("units_whole"),
    )
)

enriched = (
    base
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.store_id",
        "src.product_id",
        "src.revenue",
        "src.units_whole",
        F.col("p.product_name"),
    )
)

deduped = (
    enriched
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

metrics = (
    deduped
    .filter(F.col("product_id") == 9)
    .select(
        "id",
        "store_id",
        "product_id",
        "revenue",
        "units_whole",
        "rn",
        F.row_number().over(w2).alias("event_seq"),
        F.percent_rank().over(w3).alias("revenue_percent_rank"),
        F.ntile(4).over(w_running).alias("revenue_bucket"),
        F.sum("revenue").over(w5).alias("running_revenue"),
        F.avg("units_whole").over(w6).alias("units_whole_avg_6"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "store_id",
        "units_whole",
        "rn",
        "revenue_percent_rank",
        "revenue_bucket",
        "units_whole_avg_6",
        F.row_number().over(w7).alias("seq_no"),
        F.round((F.col("running_revenue") / F.sum("running_revenue").over(w8)), 4).alias("running_revenue_share"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("running_revenue_share") >= 0.86, "PREMIUM").otherwise("STANDARD").alias("running_revenue_share_tier"),
    )
    .agg(
        F.round(F.avg("units_whole"), 2).alias("avg_units_whole"),
        F.count("*").alias("num_rows"),
        F.avg("units_whole").alias("avg_units_whole_2"),
    )
)
