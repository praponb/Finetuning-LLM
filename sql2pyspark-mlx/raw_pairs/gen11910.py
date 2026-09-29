from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("city")
    .pivot("country", ["JP", "US"])
    .agg(F.sum("rating").alias("total"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "city",
        F.col("JP_total").alias("jp_total"),
        F.col("JP_cnt").alias("jp_cnt"),
        F.col("US_total").alias("us_total"),
        F.col("US_cnt").alias("us_cnt"),
    )
)
