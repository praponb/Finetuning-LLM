from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.when(F.col("brand").isin(["Acme", "Umbrella"]), "tier_1").otherwise("other").alias("brand_group"),
        F.quarter("launch_date").alias("launch_quarter"),
    )
    .agg(F.count("*").alias("product_count"))
    .filter(F.col("product_count") >= 10)
    .select("brand_group", "launch_quarter", "product_count")
    .orderBy(F.col("product_count").desc())
)
