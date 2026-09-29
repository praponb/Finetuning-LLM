from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("A%"))
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(
        F.sort_array(F.collect_set("region")).alias("region_list"),
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
        F.round(((100 * F.sum(F.when(F.col("region") == "East", 1).otherwise(0))) / F.count("*")), 1).alias("pct_east"),
    )
    .filter(F.col("pct_east") >= 36)
)
