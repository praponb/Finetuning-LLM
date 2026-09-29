from pyspark.sql import functions as F

base = (
    df_store_sales
    .filter((F.col("sale_date") >= F.lit("2023-09-01").cast("date")) & (F.col("sale_date") < "2023-12-06"))
    .withColumn("units_band", F.when(F.col("units") >= 32, "High").when(F.col("units") >= 6, "Medium").otherwise("Low"))
    .withColumn("units_whole", F.ceil("units"))
)

result = (
    base
    .groupBy("store_id")
    .agg(
        F.bit_and("units").alias("units_bit_and"),
        F.corr("units", "revenue").alias("corr_units_revenue"),
        F.bit_or("units").alias("units_bit_or"),
        F.percentile_approx("units_whole", 0.9).alias("units_whole_p90"),
    )
    .filter(F.col("units_bit_or") > 1)
    .orderBy(F.col("units_whole_p90").desc_nulls_first())
)
