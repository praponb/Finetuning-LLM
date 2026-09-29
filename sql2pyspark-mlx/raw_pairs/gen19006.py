from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.date_trunc("YEAR", "signup_date").alias("year_start"), "segment")
    .agg(
        F.size(F.collect_set("segment")).alias("num_segments"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.countDistinct("city").alias("distinct_cities"),
        F.bit_and("age").alias("age_bit_and"),
    )
    .orderBy(F.col("segment").desc_nulls_last())
)
