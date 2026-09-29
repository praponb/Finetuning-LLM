from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sqft")
w2 = Window.partitionBy("region", "city")
w3 = Window.partitionBy("city").orderBy("opened_date", "id")

valid = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-05-27").cast("date")) & F.col("region").isNotNull())
    .select(
        "*",
        F.date_format("opened_date", "MMM yyyy").alias("opened_date_label"),
        F.when(F.col("sqft") >= 12932, "High").when(F.col("sqft") >= 3630, "Medium").otherwise("Low").alias("sqft_level"),
        F.coalesce("region", F.lit("OTHER")).alias("region_filled"),
    )
)

result = (
    valid
    .withColumn("sqft_dense_rank", F.dense_rank().over(w))
    .withColumn("region_max_sqft", F.max("sqft").over(w2))
    .withColumn("prev_sqft", F.lag("sqft", 1).over(w3))
)
