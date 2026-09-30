from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country") == "US") & F.col("city").isin(["Tokyo", "Singapore"]))
    .groupBy("city")
    .agg(
        F.count("*").alias("num_suppliers"),
        F.avg("rating").alias("avg_rating"),
        F.countDistinct("rating").alias("unique_citys"),
    )
)
