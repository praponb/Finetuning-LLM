from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("city").isin(["Tokyo", "London", "Berlin"]) & F.col("rating").between(3.4, 3.7) & (F.length("supplier_name") < 6))
    .groupBy(
        F.when(F.col("rating") >= 3.5, "high").when(F.col("rating") >= 2.4, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("city") == "Bangkok", F.col("rating")).otherwise(0)).alias("bangkok_rating"),
    )
)
