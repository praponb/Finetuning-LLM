from pyspark.sql import functions as F

src = (
    df_stores
    .filter((F.col("sqft") < 11853) & (F.upper("store_name").like("%LD %") | F.col("store_name").like("Ri%")))
    .withColumn("sqft_band", F.when(F.col("sqft") >= 16298, "High").when(F.col("sqft") >= 5993, "Medium").otherwise("Low"))
)

region_summary = (
    src
    .groupBy("region")
    .agg(F.sum("sqft").alias("total_sqft"))
)

result = region_summary.filter(F.col("total_sqft") < 74000)
