from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("store_name").isNotNull() & F.lower("store_name").like("%irp%")) | ~F.col("city").isin(["Singapore", "New York", "London"]))
    .withColumn("sqft_rounded", F.round("sqft", 2))
    .orderBy("opened_date", "id")
    .limit(3)
)
