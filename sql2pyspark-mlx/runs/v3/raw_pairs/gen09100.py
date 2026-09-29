from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("city") != "Berlin") & F.col("region").isin(["West", "South", "North"]))
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(F.count("*").alias("total_count"))
    .filter(F.col("total_count") > 1)
)
