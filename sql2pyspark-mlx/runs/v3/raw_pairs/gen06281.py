from pyspark.sql import functions as F

base = df_suppliers.filter(F.col("city").isin(["London", "Tokyo"]))

suppliers_stats = (
    base
    .groupBy("city")
    .agg(
        F.countDistinct("rating").alias("unique_citys"),
        F.min("rating").alias("min_rating"),
    )
)

result = suppliers_stats.select("city", "min_rating")
