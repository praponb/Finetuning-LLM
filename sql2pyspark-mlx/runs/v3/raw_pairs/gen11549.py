from pyspark.sql import functions as F

filtered_suppliers = df_suppliers.filter((F.col("rating") < 3.7) & F.col("supplier_name").like("Si%"))

summary = (
    filtered_suppliers
    .groupBy(
        F.when(F.col("rating") >= 4.4, "high").when(F.col("rating") >= 1.5, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.min("rating").alias("min_rating"))
)

result = summary.orderBy("rating_bucket")
