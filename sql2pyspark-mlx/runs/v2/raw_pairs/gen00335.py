from pyspark.sql import functions as F

result = (
    df_stores
    .select("id", "store_name", "city", F.year("opened_date").alias("opened_date_year"))
    .orderBy(F.col("opened_date_year").desc(), "id")
    .limit(50)
)
