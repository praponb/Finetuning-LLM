from pyspark.sql import functions as F

base = df_suppliers.filter((F.col("country") == "UK") & F.col("rating").between(2.6, 3.6))

agg = (
    base
    .groupBy("city")
    .agg(
        F.count("rating").alias("rating_count"),
        F.sum("rating").alias("total_rating"),
    )
)

result = agg
