from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_sessions
    .withColumn("is_new_session", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = (
    with_gaps
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        F.sum("is_new_session").over(w2).alias("island_id"),
    )
)

result = (
    sessionized
    .select("id", "browser")
    .orderBy(F.col("browser").asc_nulls_last(), F.col("id").desc())
)
