from pyspark.sql import functions as F

base = (
    df_products
    .filter((F.col("stock_qty") <= 368) & F.col("supplier_id").isin([14, 33]))
)

summary = (
    base
    .groupBy(
        F.when(F.col("cost") >= 640, "high").when(F.col("cost") >= 320, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(F.count(F.when(F.col("category") == "Home", 1)).alias("home_count"))
)

result = (
    summary
    .select("cost_bucket", "home_count")
    .orderBy(F.col("home_count").desc(), "cost_bucket")
    .limit(5)
)
