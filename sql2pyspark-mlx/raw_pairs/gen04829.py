from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("week", "opened_date"), "city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("region")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("region").orderBy("cumulative_sqft", "id")
w5 = Window.partitionBy("region", "city").orderBy("opened_date", "id")

scored = (
    df_stores
    .filter(F.col("store_name").like("R%"))
    .select(
        "*",
        F.sum("sqft").over(w).alias("cumulative_sqft"),
        F.sum("sqft").over(w2).alias("region_sum_sqft"),
    )
)

with_last = (
    scored
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.amount").alias("order_amount"), F.col("c.shipping_fee"))
)

scored_stores = (
    with_last
    .withColumn("prev_order_amount", F.lag("order_amount").over(w4))
    .withColumn("next_order_amount", F.lead("order_amount").over(w4))
    .withColumn("next_sqft", F.lead("sqft").over(w4))
    .withColumn("prev_sqft", F.lag("sqft").over(w4))
    .withColumn("seq_no", F.row_number().over(w4))
)

deduped = (
    scored_stores
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        "sqft",
        "cumulative_sqft",
        "region_sum_sqft",
        "shipping_fee",
        "seq_no",
    )
)
