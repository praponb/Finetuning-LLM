from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("T%") & ~F.col("supplier_name").like("To%"))
    .groupBy(
        "city",
        "country",
        F.when(F.col("rating") >= 4.0, "high").when(F.col("rating") >= 1.8, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.count("*").alias("num_suppliers"),
        F.max("rating").alias("max_rating"),
    )
)
