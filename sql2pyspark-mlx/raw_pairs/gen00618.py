from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())

latest_match = (
    df_orders
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.store_id", "s.order_date", "s.discount", F.col("c.quantity"))
)

result = (
    latest_match
    .groupBy("store_id", F.date_trunc("WEEK", "order_date").alias("week_start"))
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.percentile_approx("quantity", 0.9).alias("quantity_p90"),
    )
    .orderBy(F.col("distinct_ids").desc())
)
