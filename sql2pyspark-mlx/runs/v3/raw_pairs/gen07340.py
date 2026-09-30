from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("%City%"))
    .groupBy("country")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "Bangkok", 1).otherwise(0))) / F.count("*")), 1).alias("pct_bangkok"),
        F.min("rating").alias("min_rating"),
    )
    .filter(F.col("pct_bangkok") < 62)
)
