from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("hour_window").orderBy(F.col("span_minutes").desc())

exploded = (
    df_carts
    .select(
        "id",
        "created_at",
        F.explode_outer("attributes").alias("attr_key", "value"),
    )
)

result = (
    exploded
    .groupBy(F.date_trunc("hour", "created_at").alias("hour_window"), "attr_key")
    .agg(
        F.count("*").alias("row_count"),
        F.size(F.collect_set("attr_key")).alias("num_attr_keys"),
        F.min("created_at").alias("first_seen"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max("created_at").alias("ended_at"),
    )
    .filter(F.col("num_attr_keys") > 1)
    .withColumn("span_minutes_rank", F.rank().over(w))
    .orderBy(F.col("distinct_ids").desc_nulls_first())
)
