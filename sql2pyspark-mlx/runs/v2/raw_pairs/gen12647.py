from pyspark.sql import functions as F

base = df_stores.filter(F.col("sqft").between(8685, 13382))

region_summary = (
    base
    .groupBy("region", "city")
    .agg(
        F.countDistinct("sqft").alias("unique_citys"),
        F.count("*").alias("row_count"),
    )
)

result = (
    region_summary
    .select("region", "city", "row_count")
    .orderBy(F.col("row_count").desc())
)
