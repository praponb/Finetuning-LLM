from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "customer_id").orderBy("amount_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("amount_rounded").desc(), "id")
w3 = Window.orderBy(F.col("shipping_fee").asc_nulls_last())

valid = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-10-02").cast("date")) & F.col("store_id").isNotNull())
    .withColumn("amount_rounded", F.round("amount", 2))
)

with_dims = (
    valid
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "inner")
    .filter(F.col("b.age") >= 60)
    .select("s.*", F.col("b.signup_date"), F.col("b.city"), F.col("b.credit_limit"))
)

eligible = (
    with_dims
    .alias("s")
    .join(df_payments.alias("x"), (F.col("x.order_id") == F.col("s.id")) & (F.col("x.method") != "bank_transfer"), "left_semi")
)

with_dims_orders = (
    eligible
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .select("a.*", F.col("c.name"), F.col("c.email"))
)

enriched = (
    with_dims_orders
    .withColumn("cumulative_credit_limit", F.sum("credit_limit").over(w))
    .withColumn("amount_rounded_bucket", F.ntile(3).over(w2))
    .withColumn("shipping_fee_percent_rank", F.percent_rank().over(w3))
)

result = (
    enriched
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "amount",
        "discount",
        "shipping_fee",
        "amount_rounded",
        F.when(F.col("status").rlike("^(COMPLETED|PENDING)$"), "GROUP_A").otherwise("GROUP_B").alias("status_zone"),
    )
    .orderBy(F.col("shipping_fee").asc_nulls_last(), F.col("discount").desc())
)
