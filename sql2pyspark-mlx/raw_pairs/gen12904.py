from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipped_date").desc(), F.col("c.id").desc())

not_flagged = (
    df_orders
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.cost") > 96), "left_semi")
)

enriched = (
    not_flagged
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id") == F.col("d.id"), "inner")
    .filter(F.col("d.opened_date") >= "2024-02-12")
    .select("s.*", F.col("d.store_name"), F.col("d.sqft"))
)

with_dims = (
    enriched
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "inner")
    .select("a.*", F.col("d.email"), F.col("d.signup_date"))
)

with_last = (
    with_dims
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.shipped_date") <= F.col("s.order_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.cost").alias("shipment_cost"))
)

result = (
    with_last
    .select(
        "id",
        "order_date",
        "status",
        "amount",
        "shipping_fee",
        "sqft",
        "shipment_cost",
        F.when(F.col("status").rlike("^(PENDING|COMPLETED)$"), "GROUP_B").when(F.col("status").rlike("^(CANCELLED|SHIPPED)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("status_zone"),
    )
)
