from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["SG", "JP"]))
    .groupBy(
        F.when(F.col("rating") >= 4.0, "high").when(F.col("rating") >= 1.5, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.countDistinct("rating").alias("unique_countrys"))
)
