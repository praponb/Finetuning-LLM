from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("city").orderBy(F.col("opened_date").desc(), "id")
w_recent = Window.orderBy("opened_date", "id")
w3 = Window.partitionBy("city").orderBy(F.col("sqft").desc_nulls_first())
w4 = Window.partitionBy(F.date_trunc("YEAR", "opened_date"), "region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("city").orderBy("event_seq", "id")
w6 = Window.partitionBy("city")
w7 = Window.partitionBy("region", "city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("city").orderBy(F.col("event_seq_2").asc_nulls_first())
w9 = Window.partitionBy("city").orderBy(F.col("latest_rank").desc(), "id")

metrics = (
    df_stores
    .withColumn("event_seq", F.row_number().over(w_hist))
    .withColumn("event_seq_2", F.row_number().over(w_recent))
    .withColumn("sqft_percent_rank", F.percent_rank().over(w3))
    .withColumn("cumulative_sqft", F.sum("sqft").over(w4))
)

latest = (
    metrics
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
)

enriched = (
    latest
    .select(
        "id",
        "city",
        "opened_date",
        "sqft",
        "event_seq_2",
        "sqft_percent_rank",
        "cumulative_sqft",
        "row_num",
        F.round((F.col("sqft") / F.sum("sqft").over(w6)), 4).alias("sqft_share"),
        F.first("sqft", ignorenulls=True).over(w7).alias("first_known_sqft"),
    )
)

latest_stores = (
    enriched
    .withColumn("latest_rank", F.rank().over(w8))
    .filter(F.col("latest_rank") == 1)
)

deduped = (
    latest_stores
    .withColumn("row_num_2", F.row_number().over(w9))
    .filter(F.col("row_num_2") == 1)
)

kept = (
    deduped
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.order_date") >= F.col("s.opened_date")), "left_anti")
)

result = (
    kept
    .select(
        "id",
        "city",
        "sqft",
        "event_seq_2",
        "row_num",
        "latest_rank",
        "row_num_2",
        F.when(F.col("city").rlike("^(Bangkok|Berlin)$"), "OTHER_ZONE").when(F.col("city").rlike("^(Chiang Mai|New York)$"), "PRIORITY").otherwise("GROUP_B").alias("city_zone"),
    )
    .orderBy(F.col("id").asc_nulls_first(), F.col("city").desc())
    .limit(10)
)
