from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("device")
w_grp = Window.partitionBy("customer_id").orderBy("is_uk", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("device").orderBy("is_uk", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.orderBy("is_uk", "id")
w6 = Window.partitionBy("web_event_event_type").orderBy(F.col("is_uk").desc_nulls_first())
w7 = Window.partitionBy("customer_id").orderBy(F.col("prev_is_uk").desc_nulls_last(), "id")

valid = (
    df_sessions
    .filter(F.col("browser").isin(["Edge", "Firefox"]))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        F.when(F.col("country") == "UK", 1).otherwise(0).alias("is_uk"),
    )
)

scored = (
    valid
    .select(
        "*",
        F.max("is_uk").over(w_hist).alias("device_max_is_uk"),
        F.last("is_uk", ignorenulls=True).over(w_grp).alias("last_known_is_uk"),
        F.sum("is_uk").over(w_grp).alias("cumulative_is_uk"),
    )
)

eligible = (
    scored
    .alias("s")
    .join(df_web_events.alias("x"), (F.col("x.session_id") == F.col("s.id")) & (F.col("x.session_id").isin([11, 15, 17, 35])), "left_anti")
)

enriched = (
    eligible
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id").eqNullSafe(F.col("c.id")), "left")
    .select("e.id", "e.customer_id", "e.device", "e.is_uk", F.col("c.email"))
)

latest_match = (
    enriched
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.event_type").alias("web_event_event_type"))
)

enriched_sessions = (
    latest_match
    .select(
        "*",
        F.avg("is_uk").over(w4).alias("is_uk_avg_2"),
        F.lag("is_uk", 1).over(w5).alias("prev_is_uk"),
        F.rank().over(w6).alias("is_uk_rank"),
    )
)

ranked = (
    enriched_sessions
    .withColumn("row_num", F.row_number().over(w7))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .rollup(F.col("d.country").alias("country"), F.col("a.device").alias("device"))
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("a.is_uk")).alias("max_is_uk"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("cnt").asc_nulls_last())
)
