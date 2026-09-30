from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "outer")
    .join(df_customers.alias("customers"), F.col("orders.customer_id") == F.col("customers.id"), "inner")
    .groupBy(
        F.col("stores.region"),
        F.quarter(F.col("stores.opened_date")).alias("opened_quarter"),
    )
    .agg(
        F.count(F.when(F.col("orders.channel") == "partner", 1)).alias("partner_count"),
        F.sum(F.when(F.col("orders.channel") == "partner", F.col("orders.amount")).otherwise(0)).alias("partner_amount"),
    )
    .orderBy(F.col("opened_quarter").desc(), F.col("partner_amount").desc())
)
