from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("uk_events").desc())

parsed = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "ship_to",
        "attributes",
        F.col("ship_to.country").alias("ship_country"),
        F.col("attributes")["weight_kg"].try_cast("double").alias("weight_kg"),
    )
)

result = (
    parsed
    .groupBy("customer_id", F.date_trunc("day", "created_at").alias("activity_day"))
    .agg(
        F.count_if(F.col("ship_country") == "UK").alias("uk_events"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
        F.sort_array(F.collect_set("ship_country")).alias("ship_country_list"),
    )
    .withColumn("uk_events_dense_rank", F.dense_rank().over(w))
)
