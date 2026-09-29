from pyspark.sql import functions as F

filtered_suppliers = (
    df_suppliers
    .filter(~F.col("city").isin(["Tokyo", "New York"]) | (F.col("rating") < 2.5))
)

suppliers_stats = (
    filtered_suppliers
    .groupBy("city")
    .agg(
        (F.max("rating") - F.min("rating")).alias("rating_range"),
        F.countDistinct("rating").alias("unique_citys"),
    )
)

result = (
    suppliers_stats
    .orderBy(F.col("rating_range").desc(), "city")
    .limit(3)
)
