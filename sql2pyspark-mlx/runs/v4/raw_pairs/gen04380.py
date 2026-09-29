from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("signup_date", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("country", "city").orderBy("credit_limit", "id")
w3 = Window.partitionBy("country").orderBy("signup_date_period", "id")
w4 = Window.partitionBy("country").orderBy("signup_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_customers
    .filter(F.upper(F.trim("name")) == "MIA ROSSI")
    .select(
        "id",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        F.date_trunc("year", "signup_date").alias("signup_date_period"),
        F.coalesce("city", F.lit("DIRECT")).alias("city_clean"),
    )
)

enriched = (
    valid
    .withColumn("credit_limit_avg_5", F.avg("credit_limit").over(w))
    .withColumn("credit_limit_bucket", F.ntile(4).over(w2))
)

gaps = (
    enriched
    .withColumn("is_new_burst", F.when(F.col("signup_date_period") - F.lag("signup_date_period").over(w3) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands = gaps.withColumn("burst_id", F.sum("is_new_burst").over(w4))

with_gaps = (
    islands
    .withColumn("new_group_flag", F.when(F.lag("signup_date_period").over(w3).isNull() | (F.col("signup_date_period") > F.lag("signup_date_period").over(w3) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_customers = with_gaps.withColumn("burst_id_2", F.sum("new_group_flag").over(w4))

result = (
    sessions_customers
    .groupBy("city")
    .pivot("segment", ["Corporate", "SMB"])
    .agg(F.avg("burst_id").alias("value_sum"))
)
