from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.year("opened_date").alias("opened_year"), "region")
    .agg(F.count("*").alias("cnt"))
    .filter(F.col("cnt") < 4)
)
