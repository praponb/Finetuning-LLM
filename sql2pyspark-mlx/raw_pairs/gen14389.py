from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("revenue").desc())
w2 = Window.partitionBy("store_id").orderBy(F.col("revenue").desc_nulls_first(), "id")
w3 = Window.partitionBy("product_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_store_sales
    .filter(F.col("revenue") >= 3300)
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "revenue",
        (F.col("units") - F.col("revenue")).alias("revenue_per_unit"),
        F.col("revenue").cast("int").alias("revenue_int"),
    )
)

ranked = (
    base
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

joined = (
    ranked
    .alias("e")
    .join(df_stores.alias("s"), F.col("e.store_id") == F.col("s.id"), "inner")
    .select("e.*", F.col("s.sqft"))
)

latest = (
    joined
    .withColumn("dedup_rank_2", F.row_number().over(w2))
    .filter(F.col("dedup_rank_2") == 1)
)

enriched = (
    latest
    .withColumn("running_avg_revenue", F.avg("revenue").over(w3))
    .withColumn("running_min_revenue", F.min("revenue").over(w3))
    .withColumn("running_sum_revenue", F.sum("revenue").over(w3))
    .withColumn("cumulative_units", F.sum("units").over(w4))
)

totals = (
    enriched
    .withColumn("running_sum_revenue_tier", F.when(F.col("running_sum_revenue") >= 17700, "PREMIUM").otherwise("STANDARD"))
    .withColumn("revenue_int_tier", F.when(F.col("revenue_int") >= 61, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["running_sum_revenue_tier", "revenue_int_tier"], ["running_sum_revenue_tier"], []],
        "running_sum_revenue_tier",
        "revenue_int_tier",
    )
    .agg(
        F.bit_or("units").alias("units_bit_or"),
        F.min("cumulative_units").alias("min_cumulative_units"),
        F.percentile_approx("revenue_int", 0.95).alias("revenue_int_p95"),
        F.corr("units", "revenue").alias("corr_units_revenue"),
        F.percentile_approx("running_min_revenue", [0.25, 0.5, 0.75]).alias("running_min_revenue_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.grouping("running_sum_revenue_tier").alias("grouping_id"),
    )
    .filter(F.col("revenue_int_p95") >= 0)
)

result = (
    totals
    .select(
        "running_sum_revenue_tier",
        "revenue_int_tier",
        "units_bit_or",
        "running_min_revenue_quartiles",
        "distinct_ids",
        "grouping_id",
    )
    .orderBy("units_bit_or", "grouping_id")
)
