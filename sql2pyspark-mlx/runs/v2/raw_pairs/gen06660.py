from pyspark.sql import functions as F

valid_suppliers = df_suppliers.filter(F.col("rating") > 3.5)

agg = (
    valid_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.5, "high").when(F.col("rating") >= 1.9, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("country") == "US", 1).otherwise(0))) / F.count("*")), 1).alias("pct_us"),
    )
)

result = agg.orderBy(F.col("pct_us").desc())
