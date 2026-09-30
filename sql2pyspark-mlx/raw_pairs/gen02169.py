from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city").orderBy(F.col("opened_date").desc(), "id")
w3 = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.orderBy("sqft", "id")
w5 = Window.partitionBy("region").orderBy(F.col("last4_avg_sqft").desc(), "id")
w6 = Window.orderBy("event_seq", "id")

with_latest = (
    df_stores
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.sale_date"))
)

deduped = (
    with_latest
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("id", "store_name", "city", "region", "sqft", "sale_date")
)

metrics = (
    deduped
    .filter(F.lower("store_name").like("%all%"))
    .withColumn("last4_avg_sqft", F.avg("sqft").over(w3))
    .withColumn("last4_sum_sqft", F.sum("sqft").over(w3))
    .withColumn("last4_max_sqft", F.max("sqft").over(w3))
    .withColumn("event_seq", F.row_number().over(w4))
)

not_flagged = (
    metrics
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.customer_id").isNull()), "left_semi")
)

kept = (
    not_flagged
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.sale_date") >= F.col("s.sale_date")), "left_anti")
)

result = (
    kept
    .select(
        "id",
        "store_name",
        "region",
        "sqft",
        "sale_date",
        "last4_avg_sqft",
        "last4_max_sqft",
        F.row_number().over(w5).alias("seq_no"),
        F.ntile(3).over(w6).alias("event_seq_bucket"),
    )
)
