from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("product_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy(F.col("greatest_units_revenue").desc(), "id")
w3 = Window.partitionBy("store_id").orderBy("units")
w4 = Window.partitionBy("store_id").orderBy(F.col("running_avg_revenue").desc_nulls_first(), "id")

base = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-07-19").cast("date"))
    .select(
        "*",
        F.greatest("units", "revenue").alias("greatest_units_revenue"),
        (F.col("revenue") - F.col("units")).alias("revenue_per_unit"),
    )
)

metrics = (
    base
    .withColumn("running_sum_revenue", F.sum("revenue").over(w_grp))
    .withColumn("running_max_revenue", F.max("revenue").over(w_grp))
    .withColumn("running_avg_revenue", F.avg("revenue").over(w_grp))
    .withColumn("running_min_revenue", F.min("revenue").over(w_grp))
    .withColumn("greatest_units_revenue_bucket", F.ntile(4).over(w2))
)

latest = (
    metrics
    .withColumn("rn", F.rank().over(w3))
    .filter(F.col("rn") == 1)
)

deduped = (
    latest
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

with_dims = (
    deduped
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "left")
    .filter(F.col("d.stock_qty") >= 298)
    .select(
        "src.id",
        "src.store_id",
        "src.product_id",
        "src.units",
        "src.revenue",
        "src.running_sum_revenue",
        "src.running_avg_revenue",
        "src.running_min_revenue",
        "src.greatest_units_revenue_bucket",
        "src.rn",
        "src.dedup_rank",
        F.col("d.category"),
        F.col("d.product_name"),
    )
)

joined = (
    with_dims
    .alias("e")
    .join(df_stores.alias("s"), F.col("e.store_id") == F.col("s.id"), "left")
    .select(
        "e.id",
        "e.store_id",
        "e.revenue",
        "e.running_sum_revenue",
        "e.greatest_units_revenue_bucket",
        "e.rn",
        "e.product_name",
        F.col("s.opened_date"),
    )
)

result = (
    joined
    .groupBy(
        F.when(F.col("revenue") >= 4400, "HIGH").otherwise("LOW").alias("revenue_tier"),
        F.date_trunc("WEEK", "opened_date").alias("week_start"),
        F.when(F.col("running_sum_revenue") >= 15900, "HIGH").otherwise("LOW").alias("running_sum_revenue_tier"),
    )
    .agg(
        F.sum("revenue").alias("total_revenue"),
        F.percentile_approx("greatest_units_revenue_bucket", 0.5).alias("greatest_units_revenue_bucket_p50"),
    )
    .filter(F.col("total_revenue") < 1000000)
    .orderBy(F.col("revenue_tier").desc())
)
