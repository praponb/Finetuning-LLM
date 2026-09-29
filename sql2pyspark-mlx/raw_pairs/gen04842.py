from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date", "id")
w2 = Window.partitionBy("region")
w3 = Window.partitionBy("region", "city")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.order_date").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("city").orderBy(F.col("sqft").desc_nulls_first(), "id")

enriched = (
    df_stores
    .filter(F.col("sqft") >= 14775)
    .select(
        "id",
        "store_name",
        "city",
        "opened_date",
        "sqft",
        F.lag("sqft", 1, 0).over(w).alias("prev_sqft"),
        F.max("sqft").over(w2).alias("region_max_sqft"),
        F.round((F.col("sqft") / F.sum("sqft").over(w3)), 4).alias("sqft_share"),
    )
)

eligible = (
    enriched
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.store_id") == 40), "left_anti")
)

filtered = (
    eligible
    .filter(F.length("store_name") > 5)
    .withColumn("sqft_share_plus_region_max_sqft", (F.col("sqft_share") + F.col("region_max_sqft")))
)

latest_match = (
    filtered
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.order_date") <= F.col("s.opened_date")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.order_date"))
)

kept = (
    latest_match
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.order_date") < F.col("s.opened_date")), "left_semi")
)

ranked = (
    kept
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "store_name",
        "city",
        "opened_date",
        "sqft",
        "region_max_sqft",
        "order_date",
    )
)

result = (
    ranked
    .select(
        "id",
        "city",
        "opened_date",
        "sqft",
        "region_max_sqft",
        F.when(F.col("city").rlike("^(Singapore|London)$"), "GROUP_B").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "GROUP_A").otherwise("EXTENDED").alias("city_zone"),
    )
    .orderBy(F.col("city_zone").desc_nulls_last())
)
