from pyspark.sql import functions as F

result = (
    df_store_sales
    .withColumn("units_filled", F.coalesce("units", F.lit(0)))
    .withColumn("units_adjusted", F.round((F.col("units") * 1.07), 2))
    .orderBy("units", F.col("id").desc())
)
