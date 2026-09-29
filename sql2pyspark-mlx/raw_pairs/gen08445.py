from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("%Old%") & (F.col("city") == "Bangkok"))
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(
        F.min("opened_date").alias("first_opened_date"),
        F.count("*").alias("num_stores"),
    )
    .orderBy("first_opened_date", F.col("opened_month").desc())
)
