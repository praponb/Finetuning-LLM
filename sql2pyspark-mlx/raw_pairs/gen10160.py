from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.order_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city").orderBy(F.col("sqft").asc_nulls_first())
w3 = Window.partitionBy("city", "status").orderBy(F.col("sqft").desc(), "id")
w4 = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(-5, -1)
w5 = Window.partitionBy("city").orderBy("sqft_dense_rank", "id")
w6 = Window.partitionBy("status", "city")
w7 = Window.partitionBy("city", "status")

filtered = (
    df_stores
    .filter(F.col("region").isNotNull() & F.col("store_name").like("%Airpo%"))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.when(F.col("region").isin(["South", "North"]), "Priority").otherwise("Other").alias("region_group"),
        F.coalesce("region", F.lit("UNKNOWN")).alias("resolved_region"),
    )
)

with_latest = (
    filtered
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.order_date") <= F.col("s.opened_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.store_name", "s.city", "s.sqft", F.col("c.status"))
)

metrics = (
    with_latest
    .select(
        "*",
        F.dense_rank().over(w2).alias("sqft_dense_rank"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "store_name",
        "city",
        "status",
        "sqft_dense_rank",
        F.avg("seq_no").over(w4).alias("seq_no_avg_5_prior"),
        F.ntile(10).over(w5).alias("sqft_dense_rank_bucket"),
        F.min("sqft_dense_rank").over(w6).alias("status_min_sqft_dense_rank"),
        F.round((F.col("sqft") / F.sum("sqft").over(w7)), 4).alias("sqft_share"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "store_name",
        "city",
        "sqft_dense_rank",
        "seq_no_avg_5_prior",
        "sqft_dense_rank_bucket",
        "status_min_sqft_dense_rank",
        "sqft_share",
        F.when(F.col("city").rlike("^(Chiang Mai|London)$"), "OTHER_ZONE").when(F.col("city").rlike("^(Bangkok|New York)$"), "CORE").otherwise("GROUP_A").alias("city_zone"),
    )
)
