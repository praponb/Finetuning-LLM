from pyspark.sql import functions as F

base = df_suppliers.filter((F.col("rating") >= 3.0) & F.col("country").isin(["US", "TH"]))

city_summary = (
    base
    .groupBy("city")
    .agg(F.count(F.when(F.col("country") == "TH", 1)).alias("th_count"))
)

result = (
    city_summary
    .filter(F.col("th_count") > 9)
    .orderBy("city", F.col("th_count").desc())
)
