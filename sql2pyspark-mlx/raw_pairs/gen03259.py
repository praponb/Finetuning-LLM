from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(
        F.count(F.when(F.col("region") == "East", 1)).alias("east_count"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") > 4)
    .select("opened_year", "east_count", "row_count")
    .orderBy("east_count")
)
