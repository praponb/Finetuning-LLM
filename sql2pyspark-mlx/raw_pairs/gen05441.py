from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.min("amount").alias("min_order_amount"))

gaps = (
    df_sessions
    .withColumn("is_new_burst", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

islands = (
    gaps
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        F.sum("is_new_burst").over(w2).alias("session_id"),
    )
)

result = (
    islands
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "country",
        "session_id",
        F.when(F.col("country").rlike("^(TH|SG)$"), "EXTENDED").when(F.col("country").rlike("^(US|JP)$"), "PRIORITY").otherwise("CORE").alias("country_zone"),
        F.col("min_order_amount"),
    )
    .orderBy(F.col("country").desc(), F.col("min_order_amount").desc())
)
