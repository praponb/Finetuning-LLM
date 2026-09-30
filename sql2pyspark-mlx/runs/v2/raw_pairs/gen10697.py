from pyspark.sql import functions as F

valid_suppliers = (
    df_suppliers
    .filter(~F.col("country").isin(["JP", "DE", "UK"]) & (F.lower(F.trim("supplier_name")) == "global supply"))
)

agg = (
    valid_suppliers
    .groupBy(
        F.when(F.col("rating") >= 3.6, "high").when(F.col("rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"),
        "country",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min("rating").alias("min_rating"),
    )
)

result = agg.select("rating_bucket", "country", "cnt", "min_rating")
