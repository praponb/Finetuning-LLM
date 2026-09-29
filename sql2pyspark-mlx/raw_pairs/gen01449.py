from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city")
    .agg(
        (F.max("age") - F.min("age")).alias("age_range"),
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_count"),
    )
    .filter(F.col("th_count") >= 3)
    .select("city", "age_range", "th_count")
    .orderBy(F.col("th_count").desc())
)
