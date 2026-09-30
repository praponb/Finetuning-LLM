from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").desc(), "id")
w_ordered = Window.partitionBy("city").orderBy("opened_date", "id")
w3 = Window.partitionBy("city").orderBy(F.col("sqft").desc())
w4 = Window.partitionBy("city")
w5 = Window.partitionBy(F.date_trunc("WEEK", "opened_date"), "city").orderBy(F.col("days_since_opened_date").desc())
w6 = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(-6, -1)
w7 = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy(F.month("opened_date"), "city").orderBy("sqft_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("city").orderBy("sqft_bucket_avg_6_prior", "id")
w10 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())

valid = (
    df_stores
    .filter((F.year("opened_date") >= 2024) & (~F.col("region").eqNullSafe(F.lit("South"))))
    .select(
        "id",
        "store_name",
        "city",
        "opened_date",
        "sqft",
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
    )
)

metrics = (
    valid
    .withColumn("sqft_bucket", F.ntile(10).over(w))
    .withColumn("next_days_since_opened_date", F.lead("days_since_opened_date", 1).over(w_ordered))
    .withColumn("sqft_rank", F.rank().over(w3))
    .withColumn("sqft_share", F.round((F.col("sqft") / F.sum("sqft").over(w4)), 4))
)

eligible = (
    metrics
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.last_restock") <= F.col("s.opened_date")), "left_anti")
)

enriched = (
    eligible
    .filter(F.col("opened_date") >= "2025-05-04")
    .select(
        "*",
        F.dense_rank().over(w5).alias("days_since_opened_date_dense_rank"),
        F.avg("sqft_bucket").over(w6).alias("sqft_bucket_avg_6_prior"),
        F.first("sqft_bucket", ignorenulls=True).over(w7).alias("first_known_sqft_bucket"),
        F.sum("sqft").over(w8).alias("running_sqft"),
    )
)

not_flagged = (
    enriched
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.sale_date") <= F.col("s.opened_date")), "left_anti")
)

ranked = (
    not_flagged
    .withColumn("rank_in_group", F.row_number().over(w9))
    .filter(F.col("rank_in_group") == 1)
)

with_latest = (
    ranked
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w10))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.quantity"),
        F.col("c.last_restock").alias("inventory_last_restock"),
    )
)

result = (
    with_latest
    .select(
        "id",
        "city",
        "sqft",
        "days_since_opened_date",
        "sqft_bucket",
        "sqft_rank",
        "sqft_share",
        "days_since_opened_date_dense_rank",
        "sqft_bucket_avg_6_prior",
        F.when(F.col("city").rlike("^(Tokyo|Berlin)$"), "EXTENDED").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "GROUP_A").otherwise("CORE").alias("city_zone"),
    )
)
