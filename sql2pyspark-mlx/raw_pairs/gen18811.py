from pyspark.sql import functions as F

result = (
    df_stores
    .withColumn("sqft_filled", F.coalesce("sqft", F.lit(0)))
    .withColumn("region_store_name", F.concat("region", F.lit(" "), "store_name"))
    .orderBy(F.col("sqft").desc())
)
