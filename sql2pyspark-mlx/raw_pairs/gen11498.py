from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "store_id")
w2 = Window.partitionBy("channel")
w3 = Window.partitionBy("customer_id").orderBy("order_date", "id").rowsBetween(-6, -1)

filtered = (
    df_orders
    .filter((F.col("order_date") > F.to_date(F.lit("2023-04-09"))) & F.col("store_id").isNotNull())
    .select("*", F.lower("status").alias("status_lower"))
)

result = (
    filtered
    .filter(F.year("order_date") == 2025)
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "shipping_fee",
        "status_lower",
        F.round((F.col("amount") / F.sum("amount").over(w)), 4).alias("amount_share"),
        F.corr("shipping_fee", "amount").over(w2).alias("corr_shipping_fee_amount"),
        F.avg("discount").over(w3).alias("discount_avg_6_prior"),
    )
)
