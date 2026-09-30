from pyspark.sql import functions as F

base = (
    df_stores
    .filter((F.col("sqft") <= 5713) & (F.col("city") == "Tokyo") & ~F.col("city").isin(["Chiang Mai", "New York", "Tokyo"]))
    .withColumn("opened_date_label", F.date_format("opened_date", "yyyy-MM"))
)

agg = (
    base
    .groupBy("region", F.year("opened_date").alias("opened_year"))
    .agg(F.countDistinct("sqft").alias("unique_citys"))
)

result = agg.orderBy(F.col("region").desc())
