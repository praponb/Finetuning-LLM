from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country").orderBy("started_at", "id")
w3 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
discount_by_customer_id = df_orders.groupBy("customer_id").agg(F.avg("discount").alias("avg_order_discount"))

enriched = (
    df_sessions
    .withColumn("last_known_country", F.last("country", ignorenulls=True).over(w_part))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("first_known_browser", F.first("browser", ignorenulls=True).over(w3))
)

result = (
    enriched
    .join(discount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "device",
        "country",
        "last_known_country",
        "seq_no",
        "first_known_browser",
        F.when(F.col("device").rlike("^(tablet|desktop)$"), "PRIORITY").otherwise("GROUP_A").alias("device_zone"),
        F.col("avg_order_discount"),
    )
    .orderBy(F.col("id").desc_nulls_first(), "seq_no")
)
