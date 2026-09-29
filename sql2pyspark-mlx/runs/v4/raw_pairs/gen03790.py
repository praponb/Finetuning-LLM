from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("product_id")
w3 = Window.partitionBy("store_id", "product_id").orderBy("revenue_per_unit", "id")
w4 = Window.partitionBy("store_id", "product_id").orderBy("revenue", "id")
w_part = Window.orderBy(F.col("revenue_per_unit").desc(), "id")
w6 = Window.orderBy("revenue", "id")
w7 = Window.partitionBy("store_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("store_id", "product_id")
w9 = Window.partitionBy("store_id", "product_id").orderBy("revenue_per_unit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("store_id").orderBy("revenue_bucket", "id")
w11 = Window.partitionBy("store_id").orderBy(F.col("units").desc(), "id")

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-06-11").cast("date"))
    .select("*", (F.col("units") * F.col("revenue")).alias("revenue_per_unit"))
)

scored = (
    cleaned
    .select(
        "*",
        F.sum("units").over(w).alias("running_units"),
        F.corr("revenue", "units").over(w_grp).alias("corr_revenue_units"),
        F.ntile(3).over(w3).alias("revenue_per_unit_bucket"),
        F.ntile(10).over(w4).alias("revenue_bucket"),
        F.ntile(4).over(w_part).alias("revenue_per_unit_bucket_2"),
    )
)

enriched = (
    scored
    .select(
        "*",
        F.lead("revenue_per_unit_bucket_2", 2).over(w6).alias("next_revenue_per_unit_bucket_2"),
        F.sum("revenue_per_unit_bucket_2").over(w7).alias("cumulative_revenue_per_unit_bucket_2"),
        F.corr("revenue", "units").over(w8).alias("corr_revenue_units_2"),
    )
)

with_windows = (
    enriched
    .withColumn("cumulative_units", F.sum("units").over(w9))
    .withColumn("running_units_share", F.round((F.col("running_units") / F.sum("running_units").over(w8)), 4))
    .withColumn("event_seq", F.row_number().over(w10))
)

ranked = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w11))
    .filter(F.col("rank_in_group") == 1)
)

with_dims = (
    ranked
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.cost"), F.col("p.price"), F.col("p.stock_qty"))
)

grouped = (
    with_dims
    .groupBy(
        F.when(F.col("revenue_per_unit_bucket_2") >= 7, "HIGH").otherwise("LOW").alias("revenue_per_unit_bucket_2_tier"),
    )
    .agg(
        F.min("cumulative_units").alias("min_cumulative_units"),
        F.bit_or("units").alias("units_bit_or"),
        F.bit_or("units").alias("units_bit_or_2"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("revenue_per_unit_bucket_2", 0.95).alias("revenue_per_unit_bucket_2_p95"),
    )
    .filter(F.col("units_bit_or") > 1)
)

result = (
    grouped
    .select(
        "revenue_per_unit_bucket_2_tier",
        "units_bit_or",
        "units_bit_or_2",
        "distinct_ids",
        "revenue_per_unit_bucket_2_p95",
    )
    .orderBy(F.col("distinct_ids").desc())
)
