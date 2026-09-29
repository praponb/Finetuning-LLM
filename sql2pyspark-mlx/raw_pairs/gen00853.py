from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("years_since_opened_date").desc(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.order_date").desc(), F.col("c.id").desc())
w_recent = Window.partitionBy("city").orderBy("years_since_opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("city").orderBy(F.col("years_since_opened_date").desc(), "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())

filtered = (
    df_stores
    .filter(F.col("region").isNotNull() & (F.col("region") == "North"))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        (F.datediff(F.current_date(), "opened_date") / 365).alias("years_since_opened_date"),
        F.initcap(F.trim("region")).alias("region_title"),
    )
)

ranked = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "city", "years_since_opened_date", "region_title")
)

with_last = (
    ranked
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.order_date"))
)

enriched = (
    with_last
    .select(
        "id",
        "city",
        "years_since_opened_date",
        "region_title",
        F.max("years_since_opened_date").over(w_recent).alias("running_max_years_since_opened_date"),
        F.sum("years_since_opened_date").over(w_recent).alias("running_sum_years_since_opened_date"),
        F.min("years_since_opened_date").over(w_recent).alias("running_min_years_since_opened_date"),
        F.row_number().over(w_part).alias("seq_no"),
    )
)

latest_match = (
    enriched
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.order_date"))
)

result = (
    latest_match
    .cube(
        F.when(F.col("running_sum_years_since_opened_date") >= 350, "PREMIUM").otherwise("STANDARD").alias("running_sum_years_since_opened_date_tier"),
        "city",
    )
    .agg(
        F.percentile_approx("running_max_years_since_opened_date", 0.5).alias("running_max_years_since_opened_date_p50"),
        F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_count"),
        F.countDistinct("city").alias("distinct_cities"),
        F.countDistinct("city").alias("distinct_cities_2"),
        F.countDistinct("city").alias("distinct_cities_3"),
        F.sum(F.when(F.col("city") == "London", F.col("seq_no")).otherwise(0)).alias("london_seq_no"),
    )
    .orderBy(F.col("distinct_cities").asc_nulls_last())
)
