from pyspark.sql import functions as F

revenue_by_store_id = df_store_sales.groupBy("store_id").agg(F.sum("revenue").alias("sum_store_sale_revenue"))

kept = (
    df_orders
    .alias("s")
    .join(df_shipments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.carrier").isin(["Kerry", "DHL"])), "left_anti")
)

result = (
    kept
    .join(revenue_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.col("sum_store_sale_revenue"),
    )
    .orderBy(F.col("sum_store_sale_revenue").desc_nulls_last(), "store_id")
)
