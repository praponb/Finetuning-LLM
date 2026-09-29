from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("country").isin(["DE", "US", "TH"]))
    .groupBy("country")
    .agg(
        F.countDistinct("rating").alias("unique_citys"),
        F.count("rating").alias("rating_count"),
        F.max("rating").alias("max_rating"),
    )
    .filter(F.col("unique_citys") >= 1)
)
