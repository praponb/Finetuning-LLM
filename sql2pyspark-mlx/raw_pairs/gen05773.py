from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_products.alias("p"), F.col("p.id") == F.col("i.product_id"), "outer")
    .filter(F.size(F.col("p.tags")) > 2)
    .groupBy(
        F.when(F.year(F.col("p.launch_date")) < 2024, "before_2024").when(F.year(F.col("p.launch_date")) == 2024, "2024").otherwise("after_2024").alias("launch_cohort"),
    )
    .agg(F.min(F.col("i.quantity")).alias("min_quantity"))
    .orderBy(F.col("min_quantity").desc(), F.col("launch_cohort").desc())
    .limit(50)
)
