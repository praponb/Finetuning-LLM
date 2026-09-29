from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("device", "customer_id").orderBy(F.col("started_at").desc())
w4 = Window.partitionBy("device", "customer_id").orderBy(F.col("burst_id").desc(), "id")

filtered = (
    df_sessions
    .filter(F.col("browser").isin(["Chrome", "Safari"]) & F.col("country").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        F.lower("device").alias("device_lower"),
        F.when(F.col("browser").isin(["Safari", "Firefox"]), "Core").otherwise("Other").alias("browser_group"),
    )
)

with_gaps = (
    filtered
    .withColumn("is_new_session", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessions_sessions = (
    with_gaps
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "device_lower",
        "browser_group",
        F.sum("is_new_session").over(w2).alias("burst_id"),
    )
)

latest = (
    sessions_sessions
    .withColumn("rank_in_group", F.rank().over(w3))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

ranked = (
    latest
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "customer_id", "device", "burst_id")
)

result = (
    ranked
    .select("id", "customer_id", "device")
    .orderBy("id")
)
