from pyspark.sql import functions as F

filtered_suppliers = df_suppliers.filter(F.col("city").isNull())

agg = (
    filtered_suppliers
    .groupBy(
        F.when(F.col("rating") >= 3.8, "high").when(F.col("rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum("rating").alias("total_rating"),
        F.count("*").alias("supplier_count"),
    )
)

result = agg.orderBy("rating_bucket")
