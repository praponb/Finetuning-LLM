from pyspark.sql import functions as F

base = (
    df_products
    .filter(F.array_contains("tags", "premium") & F.lower("product_name").like("%ack%") & F.month("launch_date").isin([5, 6, 12]))
)

agg = (
    base
    .groupBy(
        "category",
        F.when(F.col("cost") >= 620, "high").when(F.col("cost") >= 270, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.max("cost").alias("max_cost"),
        F.round(((100 * F.sum(F.when(F.col("category") == "Books", 1).otherwise(0))) / F.count("*")), 1).alias("pct_books"),
    )
)

result = (
    agg
    .filter(F.col("max_cost") > 690)
    .select("category", "cost_bucket", "max_cost", "pct_books")
    .orderBy("cost_bucket")
)
