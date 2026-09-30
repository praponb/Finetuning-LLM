from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy(F.date_trunc("WEEK", "sale_date"), "product_id").orderBy("revenue", "id").rowsBetween(-5, -1)
w_ordered = Window.partitionBy("product_id").orderBy("units_avg_5_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("product_id", "store_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("revenue").desc_nulls_last())
w_running = Window.orderBy("running_avg_units_avg_5_prior", "id")
w_hist = Window.orderBy(F.col("running_sum_units").asc_nulls_last())

filtered = (
    df_store_sales
    .filter((F.col("sale_date") > F.to_date(F.lit("2023-07-23"))) & F.col("units").isNotNull())
    .withColumn("units_level", F.when(F.col("units") >= 31, "High").when(F.col("units") >= 8, "Medium").otherwise("Low"))
)

enriched = (
    filtered
    .filter(F.col("store_id").isin([3, 4, 6]))
    .withColumn("units_share", F.round((F.col("units") / F.sum("units").over(w)), 4))
    .withColumn("units_avg_5_prior", F.avg("units").over(w2))
)

enriched_store_sales = (
    enriched
    .select(
        "*",
        F.max("units_avg_5_prior").over(w_ordered).alias("running_max_units_avg_5_prior"),
        F.avg("units_avg_5_prior").over(w_ordered).alias("running_avg_units_avg_5_prior"),
        F.count("units_avg_5_prior").over(w_ordered).alias("running_count_units_avg_5_prior"),
    )
)

metrics = (
    enriched_store_sales
    .select(
        "id",
        "sale_date",
        "revenue",
        "running_avg_units_avg_5_prior",
        "running_count_units_avg_5_prior",
        F.count("units").over(w_recent).alias("running_count_units"),
        F.sum("units").over(w_recent).alias("running_sum_units"),
        F.avg("units").over(w_recent).alias("running_avg_units"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "running_avg_units_avg_5_prior",
        "running_count_units_avg_5_prior",
        "running_avg_units",
        F.rank().over(w5).alias("revenue_rank"),
        F.lead("running_avg_units_avg_5_prior", 1).over(w_running).alias("next_running_avg_units_avg_5_prior"),
        F.rank().over(w_hist).alias("running_sum_units_rank"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("running_count_units_avg_5_prior") >= 13, "PREMIUM").otherwise("STANDARD").alias("running_count_units_avg_5_prior_tier"),
        F.when(F.col("running_avg_units") >= 45, "PREMIUM").otherwise("STANDARD").alias("running_avg_units_tier"),
    )
    .agg(
        F.percentile_approx("running_avg_units_avg_5_prior", [0.25, 0.5, 0.75]).alias("running_avg_units_avg_5_prior_quartiles"),
        F.round(F.avg("running_sum_units_rank"), 2).alias("avg_running_sum_units_rank"),
        F.countDistinct("id").alias("distinct_ids"),
        F.min("revenue_rank").alias("min_revenue_rank"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
)
