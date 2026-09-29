from pyspark.sql import functions as F

recent_suppliers = df_suppliers.filter(F.col("country").isin(["DE", "TH"]))

rating_bucket_summary = (
    recent_suppliers
    .groupBy(
        F.when(F.col("rating") >= 3.9, "high").when(F.col("rating") >= 1.9, "mid").otherwise("low").alias("rating_bucket"),
        "country",
    )
    .agg(F.round(F.avg("rating"), 2).alias("avg_rating"))
)

result = rating_bucket_summary.orderBy("country")
