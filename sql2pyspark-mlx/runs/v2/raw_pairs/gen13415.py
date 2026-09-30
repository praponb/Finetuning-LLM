from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("city").isin(["New York", "Chiang Mai", "London"]))
    .groupBy(
        F.when(F.col("rating") >= 3.9, "high").when(F.col("rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"),
        "city",
    )
    .agg(F.count("*").alias("cnt"))
    .orderBy("rating_bucket", "city")
    .limit(3)
)
