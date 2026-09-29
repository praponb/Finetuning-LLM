from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") == "Berlin")
    .groupBy("country")
    .agg(F.count(F.when(F.col("country") == "TH", 1)).alias("th_count"))
    .filter(F.col("th_count") > 6)
    .select("country", "th_count")
)
