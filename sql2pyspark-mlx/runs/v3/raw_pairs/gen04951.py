from pyspark.sql import functions as F

filtered_suppliers = (
    df_suppliers
    .filter(F.col("city").isin(["Bangkok", "Tokyo"]) | F.col("country").isin(["TH", "SG"]))
)

summary = (
    filtered_suppliers
    .groupBy(
        "country",
        F.when(F.col("rating") >= 4.3, "high").when(F.col("rating") >= 2.6, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.max("rating").alias("max_rating"),
        F.countDistinct("rating").alias("unique_citys"),
    )
)

result = (
    summary
    .orderBy("rating_bucket", F.col("unique_citys").desc(), "country")
    .limit(1)
)
