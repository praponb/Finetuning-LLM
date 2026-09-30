from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("revenue", "id")
w_hist = Window.partitionBy("product_id").orderBy("revenue", "id").rowsBetween(-3, Window.currentRow)
w_grp = Window.partitionBy("product_id").orderBy("days_since_sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.year("launch_date"), "product_category").orderBy(F.col("revenue").asc_nulls_last())
w5 = Window.partitionBy("product_category")
w6 = Window.partitionBy("category").orderBy("cost", "id")

valid = (
    df_store_sales
    .filter((F.col("sale_date") > F.to_date(F.lit("2023-07-10"))) & F.col("revenue").between(2400, 2900))
    .select(
        "id",
        "product_id",
        "revenue",
        F.datediff(F.current_date(), "sale_date").alias("days_since_sale_date"),
    )
)

deduped = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

enriched = (
    deduped
    .filter(F.col("product_id") == 10)
    .withColumn("last4_avg_days_since_sale_date", F.avg("days_since_sale_date").over(w_hist))
    .withColumn("last4_sum_days_since_sale_date", F.sum("days_since_sale_date").over(w_hist))
    .withColumn("last4_count_days_since_sale_date", F.count("days_since_sale_date").over(w_hist))
    .withColumn("running_revenue", F.sum("revenue").over(w_grp))
)

joined = (
    enriched
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "left")
    .select(
        "src.id",
        "src.product_id",
        "src.revenue",
        "src.last4_avg_days_since_sale_date",
        F.col("b.price"),
        F.col("b.category"),
    )
)

joined_store_sales = (
    joined
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "left")
    .select(
        "a.*",
        F.col("d.launch_date"),
        F.col("d.category").alias("product_category"),
        F.col("d.cost"),
    )
)

metrics = (
    joined_store_sales
    .select(
        "id",
        "product_id",
        "launch_date",
        F.percent_rank().over(w4).alias("revenue_percent_rank"),
        F.max("cost").over(w5).alias("product_category_max_cost"),
        F.ntile(5).over(w6).alias("cost_bucket"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("revenue_percent_rank") >= 0.64, "HIGH").otherwise("LOW").alias("revenue_percent_rank_tier"),
    )
    .agg(
        F.round(F.avg("cost_bucket"), 2).alias("avg_cost_bucket"),
        F.percentile_approx("product_category_max_cost", 0.5).alias("product_category_max_cost_p50"),
        F.min("revenue_percent_rank").alias("min_revenue_percent_rank"),
        F.percentile_approx("product_category_max_cost", 0.5).alias("product_category_max_cost_p50_2"),
        F.min("cost_bucket").alias("min_cost_bucket"),
        F.round(F.avg("cost_bucket"), 2).alias("avg_cost_bucket_2"),
    )
    .filter(F.col("avg_cost_bucket") > 0)
    .orderBy(F.col("product_category_max_cost_p50_2").asc_nulls_first())
)
