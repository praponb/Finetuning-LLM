from pyspark.sql import functions as F

kept = (
    df_orders
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.shipped_date") <= F.col("s.order_date")), "left_anti")
)

result = (
    kept
    .select(
        "id",
        "order_date",
        "status",
        "discount",
        "shipping_fee",
        F.when(F.col("status").rlike("^(COMPLETED|CANCELLED)$"), "GROUP_A").when(F.col("status").rlike("^(PENDING|SHIPPED)$"), "EXTENDED").otherwise("GROUP_B").alias("status_zone"),
    )
    .orderBy(F.col("order_date").asc_nulls_first())
)
