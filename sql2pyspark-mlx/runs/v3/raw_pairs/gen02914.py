from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~F.col("city").isin(["Tokyo", "Chiang Mai", "New York"]))
    .groupBy("country")
    .agg(F.count(F.when(F.col("city") == "London", 1)).alias("london_count"))
    .filter(F.col("london_count") >= 7)
    .select("country", "london_count")
)
