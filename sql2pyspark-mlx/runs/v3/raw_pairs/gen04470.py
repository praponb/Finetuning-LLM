from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(F.min("opened_date").alias("first_opened_date"))
    .orderBy("opened_year")
)
