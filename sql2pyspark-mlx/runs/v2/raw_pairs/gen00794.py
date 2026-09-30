from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("units") <= 26) & ((F.col("store_id") == 1) | (F.col("revenue") < 3800)))
    .withColumn("units_filled", F.coalesce("units", F.lit(0)))
    .withColumn("units_rounded", F.round("units", 2))
    .orderBy(F.col("id").desc(), "sale_date")
)
