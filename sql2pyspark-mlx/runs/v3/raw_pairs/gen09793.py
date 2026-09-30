from pyspark.sql import functions as F

src = df_suppliers.filter(F.length("supplier_name") >= 8)

suppliers_stats = (
    src
    .groupBy("country")
    .agg(
        F.sum(F.when(F.col("country") == "UK", F.col("rating")).otherwise(0)).alias("uk_rating"),
        F.sum("rating").alias("total_rating"),
    )
)

result = suppliers_stats.orderBy(F.col("country").desc())
