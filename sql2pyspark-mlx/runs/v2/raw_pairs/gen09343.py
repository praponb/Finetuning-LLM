from pyspark.sql import functions as F

result = (
    df_suppliers
    .rollup("city", "country")
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.max("rating").alias("max_rating"),
    )
)
