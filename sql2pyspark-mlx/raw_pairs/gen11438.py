from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy("opened_date", "id")
w2 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city", "region")
w4 = Window.partitionBy(F.year("opened_date")).orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())
w6 = Window.partitionBy("order_channel", "region").orderBy("shipping_fee", "id").rowsBetween(-6, Window.currentRow)

base = (
    df_stores
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-06-08"))) & F.col("region").isNotNull())
    .select(
        "*",
        F.add_months("opened_date", 1).alias("opened_date_plus_months"),
        F.concat_ws("/", "store_name", "city").alias("store_name_city_key"),
    )
)

ranked = (
    base
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

metrics = (
    ranked
    .filter(F.length("store_name") < 5)
    .select(
        "id",
        "city",
        "region",
        "opened_date_plus_months",
        "store_name_city_key",
        "row_num",
        F.sum("sqft").over(w2).alias("cumulative_sqft"),
        F.max("sqft").over(w3).alias("city_max_sqft"),
        F.sum("sqft").over(w4).alias("cumulative_sqft_2"),
    )
)

latest_match = (
    metrics
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.region",
        "s.opened_date_plus_months",
        "s.row_num",
        "s.city_max_sqft",
        F.col("c.channel").alias("order_channel"),
        F.col("c.shipping_fee"),
    )
)

metrics_stores = (
    latest_match
    .withColumn("last7_count_city_max_sqft", F.count("city_max_sqft").over(w6))
    .withColumn("last7_max_city_max_sqft", F.max("city_max_sqft").over(w6))
    .withColumn("last7_sum_city_max_sqft", F.sum("city_max_sqft").over(w6))
    .withColumn("last7_avg_city_max_sqft", F.avg("city_max_sqft").over(w6))
)

not_flagged = (
    metrics_stores
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.order_date") >= F.col("s.opened_date_plus_months")), "left_semi")
)

result = (
    not_flagged
    .withColumn("week_start", F.date_trunc("WEEK", "opened_date_plus_months"))
    .groupingSets([["week_start", "region"], ["week_start"]], "week_start", "region")
    .agg(
        F.size(F.collect_set("region")).alias("num_regions"),
        F.percentile_approx("city_max_sqft", [0.25, 0.5, 0.75]).alias("city_max_sqft_quartiles"),
        F.round(F.sum("last7_sum_city_max_sqft") / F.nullif(F.count(F.when(F.col("order_channel") == "mobile", 1)), F.lit(0)), 2).alias("last7_sum_city_max_sqft_per_mobile"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("region") == "South", 1)).alias("south_rows"),
        F.count(F.when(F.col("order_channel") == "partner", 1)).alias("partner_count"),
    )
    .filter(F.col("partner_count") >= 1)
)
