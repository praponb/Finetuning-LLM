from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category").isin(["Grocery", "Electronics"]))
    .agg(
        F.countDistinct("cost").alias("unique_suppliers"),
        F.round(((100 * F.sum(F.when(F.col("category") == "Fashion", 1).otherwise(0))) / F.count("*")), 1).alias("pct_fashion"),
    )
    .orderBy(F.col("unique_suppliers").desc())
)
