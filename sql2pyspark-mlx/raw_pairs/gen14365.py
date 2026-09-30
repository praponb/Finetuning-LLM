from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment", "city").orderBy(F.col("signup_date").desc_nulls_first(), "id")

deduped = (
    df_customers
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .groupBy(
        F.when(F.col("credit_limit") >= 15800, "PREMIUM").otherwise("STANDARD").alias("credit_limit_tier"),
        "city",
        F.when(F.col("age") >= 59, "HIGH").otherwise("LOW").alias("age_tier"),
    )
    .agg(
        F.count_if(F.col("country") == "UK").alias("uk_events"),
        F.sum(F.when(F.col("country") == "US", F.col("age")).otherwise(0)).alias("us_age"),
    )
    .orderBy(F.col("age_tier").desc_nulls_last())
)
