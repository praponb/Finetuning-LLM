from pyspark.sql import functions as F

valid_suppliers = (
    df_suppliers
    .filter(F.col("country").isin(["TH", "UK", "US"]) | (F.col("city") == "Chiang Mai"))
    .withColumn("city_upper", F.upper("city"))
)

suppliers_stats = (
    valid_suppliers
    .groupBy("city")
    .agg(F.count(F.when(F.col("country") == "US", 1)).alias("us_count"))
)

result = suppliers_stats.orderBy(F.col("us_count").desc())
