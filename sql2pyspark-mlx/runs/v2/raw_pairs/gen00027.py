from pyspark.sql import functions as F

src = df_suppliers.filter(F.col("country") == "US")

suppliers_stats = (
    src
    .groupBy(
        F.when(F.col("rating") >= 4.1, "high").when(F.col("rating") >= 1.9, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.countDistinct("rating").alias("unique_countrys"))
)

result = suppliers_stats.filter(F.col("unique_countrys") < 6)
