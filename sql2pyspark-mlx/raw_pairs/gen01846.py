from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("launch_date", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("brand", "category").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("brand").orderBy(F.col("cost").asc_nulls_first())
w4 = Window.partitionBy("category")
w5 = Window.partitionBy("brand").orderBy(F.col("cost_percent_rank").desc(), "id")

base = (
    df_products
    .filter(F.col("supplier_id").isNull() & (F.col("launch_date") >= F.to_date(F.lit("2023-06-28"))))
    .select(
        "id",
        "category",
        "brand",
        "cost",
        "launch_date",
        "tags",
        F.concat_ws(",", "category", "brand").alias("category_brand_key"),
    )
)

scored = (
    base
    .select(
        "*",
        F.avg("cost").over(w).alias("cost_avg_3"),
        F.sum("cost").over(w2).alias("running_cost"),
        F.percent_rank().over(w3).alias("cost_percent_rank"),
        F.round((F.col("cost") / F.sum("cost").over(w4)), 4).alias("cost_share"),
    )
)

filtered = (
    scored
    .filter(F.col("running_cost").between(2000, 2779))
    .select(
        "id",
        "category",
        "brand",
        "cost",
        "launch_date",
        "tags",
        "category_brand_key",
        "running_cost",
        "cost_percent_rank",
        F.ceil("cost_share").alias("cost_share_whole"),
        F.round("cost_share", 0).alias("cost_share_rounded"),
    )
)

latest = (
    filtered
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .groupBy(F.date_trunc("MONTH", "launch_date").alias("activity_month"))
    .agg(
        F.count(F.when(F.col("category") == "Books", 1)).alias("books_count"),
        F.avg(F.when(F.col("brand") == "Umbrella", F.col("cost_share_whole"))).alias("umbrella_avg_cost_share_whole"),
        F.countDistinct("brand").alias("distinct_brands"),
        F.size(F.collect_set("brand")).alias("num_brands"),
        F.count("*").alias("row_count"),
        F.max("running_cost").alias("max_running_cost"),
    )
    .orderBy(F.col("books_count").desc_nulls_last())
)
