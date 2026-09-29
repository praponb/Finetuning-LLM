from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("age_tier").orderBy(F.col("row_count").desc())

cleaned = (
    df_customers
    .filter((F.col("signup_date") >= F.to_date(F.lit("2023-04-20"))) & F.col("segment").isNotNull())
    .withColumn("days_since_signup_date", F.datediff(F.current_date(), "signup_date"))
    .withColumn("signup_date_period", F.date_trunc("week", "signup_date"))
    .withColumn("city_filled", F.coalesce("city", F.lit("DIRECT")))
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("age") >= 72, "LARGE").otherwise("SMALL").alias("age_tier"),
        "city",
    )
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.count("*").alias("row_count"),
        F.size(F.collect_set("segment")).alias("num_segments"),
    )
    .withColumn("row_count_dense_rank", F.dense_rank().over(w))
    .orderBy(F.col("city").desc_nulls_last())
)
