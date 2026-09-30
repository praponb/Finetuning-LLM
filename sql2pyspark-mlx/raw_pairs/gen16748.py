from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("city").orderBy("sqft")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())

enriched = (
    df_stores
    .filter(F.col("city").isin(["Bangkok", "London", "Berlin"]))
    .withColumn("city_avg_sqft", F.avg("sqft").over(w))
    .withColumn("sqft_percent_rank", F.percent_rank().over(w2))
)

eligible = (
    enriched
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.sale_date") <= F.col("s.opened_date")), "left_semi")
)

with_latest = (
    eligible
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.units"))
)

eligible_stores = (
    with_latest
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.order_date") < F.col("s.opened_date")), "left_semi")
)

not_flagged = (
    eligible_stores
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.order_date") < F.col("s.opened_date")), "left_anti")
)

valid = (
    not_flagged
    .filter((F.col("opened_date") > F.lit("2023-07-08").cast("date")) & (F.col("city") == "Chiang Mai"))
    .withColumn("is_bangkok", F.when(F.col("city") == "Bangkok", 1).otherwise(0))
    .withColumn("days_since_opened_date", F.datediff(F.current_date(), "opened_date"))
    .withColumn("region_filled", F.coalesce("region", F.lit("UNKNOWN")))
)

result = (
    valid
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        "sqft",
        "city_avg_sqft",
        "region_filled",
    )
)
