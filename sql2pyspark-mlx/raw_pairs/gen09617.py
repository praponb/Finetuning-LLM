from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("year_start").orderBy(F.col("london_credit_limit").desc())

matched_keys = (
    df_customers
    .select("id", "segment")
    .intersectAll(
        df_customers
        .filter(F.col("credit_limit") < 10700)
        .select("id", "segment")
    )
)

kept_rows = (
    df_customers
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.segment") == F.col("k.segment")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(F.date_trunc("year", "signup_date").alias("year_start"), "segment")
    .agg(
        F.sum(F.when(F.col("city") == "London", F.col("credit_limit")).otherwise(0)).alias("london_credit_limit"),
        F.sum(F.when(F.col("country") == "SG", F.col("credit_limit")).otherwise(0)).alias("sg_credit_limit"),
        F.countDistinct("segment").alias("distinct_segments"),
        F.round(F.avg("age"), 2).alias("avg_age"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
    .withColumn("london_credit_limit_percent_rank", F.percent_rank().over(w))
)
