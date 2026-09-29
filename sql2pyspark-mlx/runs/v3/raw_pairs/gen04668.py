from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.upper(F.trim("supplier_name")) == "PACIFIC WHOLESALE")
    .groupBy(
        F.when(F.col("city").isin(["New York", "Singapore"]), "group_a").otherwise("other").alias("city_group"),
        F.when(F.col("rating") >= 3.6, "high").when(F.col("rating") >= 1.7, "mid").otherwise("low").alias("rating_bucket"),
        "city",
    )
    .agg(
        F.min("rating").alias("min_rating"),
        F.sum("rating").alias("total_rating"),
    )
    .filter(F.col("min_rating") >= 2.6)
)
