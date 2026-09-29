from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("end_date_period").desc_nulls_first())
w2 = Window.partitionBy("status").orderBy("end_date_period", "id")
w3 = Window.partitionBy("status").orderBy("end_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_subscriptions
    .filter((F.col("end_date") >= F.lit("2023-04-06").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "*",
        F.dayofweek("start_date").alias("start_date_dayofweek"),
        F.date_trunc("YEAR", "end_date").alias("end_date_period"),
    )
)

ranked = (
    valid
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

joined = (
    ranked
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "inner")
    .select("a.*", F.col("b.country"))
)

flagged = (
    joined
    .withColumn("is_new_session", F.when(F.lag("end_date_period").over(w2).isNull() | (F.col("end_date_period") > F.lag("end_date_period").over(w2) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_subscriptions = flagged.withColumn("island_id", F.sum("is_new_session").over(w3))

result = (
    sessions_subscriptions
    .alias("src")
    .join(df_customers.alias("dim"), F.col("src.customer_id") == F.col("dim.id"), "inner")
    .cube(
        F.col("dim.country").alias("customer_country"),
        F.col("src.status").alias("status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("src.monthly_fee")).alias("min_monthly_fee"),
        F.round(F.avg(F.col("src.is_new_session")), 2).alias("avg_is_new_session"),
        F.grouping_id().alias("gid"),
    )
)
