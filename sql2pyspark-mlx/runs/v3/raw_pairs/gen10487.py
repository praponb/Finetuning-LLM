from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") < 3.5) & F.col("supplier_name").like("To%"))
    .groupBy("country")
    .agg(
        F.sum(F.when(F.col("city") == "Singapore", F.col("rating")).otherwise(0)).alias("singapore_rating"),
        F.count(F.when(F.col("country") == "SG", 1)).alias("sg_count"),
        F.sum("rating").alias("total_rating"),
    )
)
