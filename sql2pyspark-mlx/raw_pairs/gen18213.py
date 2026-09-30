from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id", "status").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy("shipping_fee", "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())

valid = (
    df_orders
    .filter((F.col("order_date") > F.lit("2023-03-13").cast("date")) & F.col("channel").isNotNull())
    .withColumn("status_title", F.initcap(F.trim("status")))
)

joined = (
    valid
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .filter(F.col("d.segment").isin(["Corporate", "SMB", "Consumer"]))
    .select(
        "s.id",
        "s.customer_id",
        "s.order_date",
        "s.status",
        "s.shipping_fee",
        "s.status_title",
        F.col("d.signup_date"),
        F.col("d.email"),
        F.col("d.credit_limit"),
    )
)

scored = (
    joined
    .select(
        "id",
        "customer_id",
        "order_date",
        "shipping_fee",
        "email",
        F.first("credit_limit", ignorenulls=True).over(w).alias("first_known_credit_limit"),
        F.first("credit_limit", ignorenulls=True).over(w2).alias("first_known_credit_limit_2"),
        F.lead("shipping_fee", 2).over(w3).alias("next_shipping_fee"),
    )
)

cleaned = (
    scored
    .filter((F.col("order_date") >= F.to_date(F.lit("2023-08-17"))) & F.col("customer_id").isNotNull())
    .select("*", F.ceil("shipping_fee").alias("shipping_fee_whole"))
)

with_dims = (
    cleaned
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .select(
        "s.id",
        "s.customer_id",
        "s.order_date",
        "s.shipping_fee",
        "s.email",
        "s.next_shipping_fee",
        "s.shipping_fee_whole",
        F.col("c.age"),
        F.col("c.credit_limit"),
    )
)

with_last = (
    with_dims
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.shipping_fee",
        "s.email",
        "s.age",
        F.col("c.shipped_date").alias("shipment_shipped_date"),
    )
)

valid_orders = (
    with_last
    .filter(F.col("shipment_shipped_date") >= "2023-09-01")
    .select(
        "id",
        "shipping_fee",
        "email",
        "age",
        "shipment_shipped_date",
        (F.col("age") - F.col("shipping_fee")).alias("age_minus_shipping_fee"),
        F.regexp_extract("email", "@(.+)$", 1).alias("email_domain"),
    )
)

result = valid_orders.select("id", "shipping_fee", "email", "age", "shipment_shipped_date")
