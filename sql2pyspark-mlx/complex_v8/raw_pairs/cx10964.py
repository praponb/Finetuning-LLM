from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("agent_id").orderBy(F.col("csat").desc(), F.col("id").desc())

prepared = (
    df_tickets
    .filter((F.month(F.col("opened_at")).isin(1, 2)) & (F.col("channel").isin("chat", "email", "phone")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "csat",
        F.substring(F.trim(F.col("channel")), 1, 3).alias("channel_prefix"),
        F.concat(F.col("channel"), F.lit("#"), F.col("requester_id").cast("string")).alias("channel_ref"),
    )
)

latest = (
    prepared
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest
    .cube("csat", "agent_id")
    .agg(F.count("*").alias("cnt"))
    .orderBy(F.col("csat").asc_nulls_last(), F.col("agent_id").asc_nulls_last())
)
