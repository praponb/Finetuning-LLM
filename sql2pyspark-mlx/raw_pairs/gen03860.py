from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id", "store_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipped_date").desc(), F.col("c.id").desc())
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.paid_at").desc(), F.col("c.id").desc())

filtered = (
    df_orders
    .filter(F.col("order_date") >= F.to_date(F.lit("2023-08-08")))
    .withColumn("order_date_month_end", F.last_day("order_date"))
    .withColumn("discount_filled", F.coalesce("discount", F.lit(0)))
)

enriched = (
    filtered
    .select(
        "id",
        "customer_id",
        "shipping_fee",
        "discount_filled",
        F.sum("amount").over(w).alias("running_amount"),
        F.last("amount", ignorenulls=True).over(w2).alias("last_known_amount"),
        F.corr("discount", "amount").over(w3).alias("corr_discount_amount"),
    )
)

with_last = (
    enriched
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.cost").alias("shipment_cost"))
)

with_dims = (
    with_last
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .filter(F.col("p.age") <= 53)
    .select("src.*", F.col("p.signup_date"), F.col("p.city"), F.col("p.segment"))
)

latest_match = (
    with_dims
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.paid_at") <= F.col("s.signup_date")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.paid_at"))
)

result = (
    latest_match
    .groupBy(
        F.when(F.col("shipment_cost") >= 75, "LARGE").otherwise("SMALL").alias("shipment_cost_tier"),
    )
    .agg(
        F.percentile_approx("running_amount", [0.25, 0.5, 0.75]).alias("running_amount_quartiles"),
        F.sum(F.when(F.col("city") == "New York", F.col("corr_discount_amount")).otherwise(0)).alias("new_york_corr_discount_amount"),
    )
    .filter(F.col("new_york_corr_discount_amount") >= 0)
    .orderBy(F.col("new_york_corr_discount_amount").desc())
)
