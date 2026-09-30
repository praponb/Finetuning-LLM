from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("L%"))
    .groupBy("country")
    .agg(
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
        F.sum(F.when(F.col("city") == "Singapore", F.col("rating")).otherwise(0)).alias("singapore_rating"),
    )
    .filter(F.col("singapore_rating") > 39)
)
