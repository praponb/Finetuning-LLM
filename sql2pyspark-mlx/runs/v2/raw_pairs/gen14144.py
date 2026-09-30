from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.lower(F.trim("store_name")) == "siam flagship")
    .groupBy("city", F.year("opened_date").alias("opened_year"))
    .agg(F.count("*").alias("total_count"))
    .filter(F.col("total_count") < 6)
    .select("city", "opened_year", "total_count")
    .orderBy(F.col("opened_year").desc(), F.col("total_count").desc())
)
