from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at_period", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_sessions
    .filter((F.col("started_at") > F.to_date(F.lit("2023-07-20"))) & F.col("browser").isNotNull())
    .select("*", F.date_trunc("YEAR", "started_at").alias("started_at_period"))
)

with_gaps = (
    base
    .withColumn("session_start_flag", F.when(F.col("started_at_period") - F.lag("started_at_period").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessions_sessions = (
    with_gaps
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at_period",
        F.sum("session_start_flag").over(w2).alias("session_id"),
    )
)

result = (
    sessions_sessions
    .groupBy(
        F.date_trunc("WEEK", "started_at_period").alias("week_start"),
        "session_id",
    )
    .agg(
        F.size(F.collect_set("browser")).alias("num_browsers"),
        F.max("started_at_period").alias("last_started_at_period"),
    )
)
