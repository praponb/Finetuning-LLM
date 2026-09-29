from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.when(F.col("brand").isin(["Acme", "Initech"]), "priority").otherwise("other").alias("brand_group"),
        "brand",
    )
    .agg(
        F.sum(F.when(F.col("category") == "Fashion", 1).otherwise(0)).alias("fashion_count"),
    )
    .filter(F.col("fashion_count") < 41)
    .select("brand_group", "brand", "fashion_count")
    .orderBy(F.col("brand_group").desc())
)
