from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~((F.col("city") != "New York") & F.month("opened_date").isin([6, 7, 12])))
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(F.min("opened_date").alias("first_opened_date"))
)
