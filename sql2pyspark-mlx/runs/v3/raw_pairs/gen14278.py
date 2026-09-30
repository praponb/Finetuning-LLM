from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & F.col("supplier_id").isNotNull())
    .cube(
        "category",
        F.when(F.col("cost") >= 750, "high").when(F.col("cost") >= 350, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.avg("cost").alias("avg_cost"),
        F.sum("cost").alias("total_cost"),
    )
    .orderBy("total_cost")
)
