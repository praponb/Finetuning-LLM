from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.date_trunc("WEEK", "sale_date").alias("week_start"))
    .agg(
        F.corr("units", "revenue").alias("corr_units_revenue"),
        F.bit_or("units").alias("units_bit_or"),
        F.max("revenue").alias("max_revenue"),
    )
    .orderBy("week_start")
)
