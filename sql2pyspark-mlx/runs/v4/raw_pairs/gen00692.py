from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country") == "US") & F.col("supplier_name").isNotNull())
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.count(F.when(F.col("country") == "SG", 1)).alias("sg_count"),
    )
)
