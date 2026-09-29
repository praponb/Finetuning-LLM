from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country") != "US") & ((F.col("country") != "US") | (F.length("supplier_name") < 9)))
    .groupBy(
        "city",
        F.when(F.col("rating") >= 3.4, "high").when(F.col("rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.avg("rating").alias("avg_rating"),
    )
)
