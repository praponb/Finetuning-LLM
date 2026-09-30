from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "created_at",
        F.explode_outer("attributes").alias("key", "attr_value"),
    )
)

result = (
    exploded
    .groupBy("key")
    .agg(
        F.countDistinct("key").alias("distinct_keys"),
        F.count(F.when(F.col("key") == "gift", 1)).alias("gift_rows"),
        F.max("created_at").alias("ended_at"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
    )
    .orderBy(F.col("duration_min").desc())
)
