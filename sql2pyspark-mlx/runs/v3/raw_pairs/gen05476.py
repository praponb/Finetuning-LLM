from pyspark.sql import functions as F

valid_suppliers = df_suppliers.filter(F.col("rating").isNotNull() | (F.col("country") == "JP"))

rating_bucket_summary = (
    valid_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.6, "high").when(F.col("rating") >= 2.5, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.countDistinct("rating").alias("unique_citys"),
        F.round(((100 * F.sum(F.when(F.col("country") == "TH", 1).otherwise(0))) / F.count("*")), 1).alias("pct_th"),
    )
)

result = (
    rating_bucket_summary
    .filter(F.col("pct_th") < 45)
    .orderBy("pct_th")
)
