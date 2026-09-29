from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("Li%"))
    .groupBy("city")
    .agg(F.sum(F.when(F.col("country") == "TH", 1).otherwise(0)).alias("th_count"))
    .filter(F.col("th_count") > 75)
    .select("city", "th_count")
    .orderBy("th_count", F.col("city").desc())
)
