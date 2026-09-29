from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") <= 15300)
    .groupBy("segment", "city")
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        (F.max("age") - F.min("age")).alias("age_range"),
    )
    .filter(F.col("age_range") < 23)
    .orderBy("segment")
)
