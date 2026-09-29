from pyspark.sql import functions as F

recent_suppliers = df_suppliers.filter(F.col("country") == "DE")

agg = (
    recent_suppliers
    .groupBy(
        "city",
        F.when(F.col("rating") >= 3.7, "high").when(F.col("rating") >= 2.1, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.count("*").alias("cnt"))
)

result = (
    agg
    .filter(F.col("cnt") > 7)
    .orderBy(F.col("city").desc())
)
