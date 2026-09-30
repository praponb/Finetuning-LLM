from pyspark.sql import functions as F

result = (
    df_products
    .filter(~(F.col("brand").isNotNull() & (F.col("brand") == "Acme")))
    .groupBy(
        F.when(F.col("category").isin(["Home", "Sports"]), "tier_1").otherwise("other").alias("category_group"),
        F.month("launch_date").alias("launch_month"),
    )
    .agg(
        F.sum(F.when(F.col("brand") == "Acme", F.col("cost")).otherwise(0)).alias("acme_cost"),
    )
    .filter(F.col("acme_cost") > 56)
    .orderBy(F.col("category_group").desc())
)
