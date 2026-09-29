from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

latest_match = (
    df_customers
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.name",
        "s.city",
        "s.credit_limit",
        F.col("c.status"),
        F.col("c.discount").alias("order_discount"),
    )
)

result = (
    latest_match
    .groupBy(
        F.when(F.col("credit_limit") >= 12200, "LARGE").otherwise("SMALL").alias("credit_limit_tier"),
    )
    .agg(
        F.countDistinct("city").alias("distinct_cities"),
        F.count_if(F.col("status") == "SHIPPED").alias("shipped_events"),
        F.avg("order_discount").alias("avg_order_discount"),
    )
    .orderBy(F.col("avg_order_discount").desc_nulls_last())
)
