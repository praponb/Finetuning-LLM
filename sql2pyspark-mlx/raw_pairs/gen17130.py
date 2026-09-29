from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("customer_id").orderBy(F.col("age").asc_nulls_first())
w3 = Window.partitionBy(F.to_date("created_at"), "attr_key")
w4 = Window.partitionBy("attr_key")
w5 = Window.partitionBy("attr_key").orderBy(F.col("age").desc_nulls_first())

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("attributes").alias("attr_key", "v"),
    )
)

with_dims = (
    exploded
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .select(
        "a.id",
        "a.customer_id",
        "a.created_at",
        "a.attr_key",
        "a.v",
        F.col("d.email"),
        F.col("d.age"),
        F.col("d.city"),
    )
)

metrics = (
    with_dims
    .withColumn("city_min_age", F.min("age").over(w))
    .withColumn("age_cume_dist", F.cume_dist().over(w2))
    .withColumn("created_at_to_date_count_age", F.count("age").over(w3))
    .withColumn("age_share", F.round((F.col("age") / F.sum("age").over(w4)), 4))
    .withColumn("age_percent_rank", F.percent_rank().over(w5))
)

valid = (
    metrics
    .filter(F.col("age_percent_rank") <= 0.64)
    .select(
        "id",
        "customer_id",
        "v",
        "age",
        "city",
        "city_min_age",
        "age_cume_dist",
        "age_share",
        F.when(F.col("age") >= 71, "High").when(F.col("age") >= 27, "Medium").otherwise("Low").alias("age_band"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("resolved_city"),
    )
)

grouped = (
    valid
    .withColumn("age_cume_dist_tier", F.when(F.col("age_cume_dist") >= 0.83, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["city", "age_cume_dist_tier"], ["city"], []],
        "city",
        "age_cume_dist_tier",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.round(F.avg("city_min_age"), 2).alias("avg_city_min_age"),
        F.avg(F.when(F.col("city") == "London", F.col("age"))).alias("london_avg_age"),
        F.round(F.avg("city_min_age"), 2).alias("avg_city_min_age_2"),
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_count"),
    )
    .filter(F.col("tokyo_count") > 1)
)

result = (
    grouped
    .select("distinct_ids", "avg_city_min_age", "london_avg_age", "avg_city_min_age_2")
)
