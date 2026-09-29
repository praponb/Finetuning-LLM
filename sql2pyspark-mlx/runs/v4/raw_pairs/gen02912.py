from pyspark.sql import functions as F

base = (
    df_orders
    .filter(F.col("channel").isNotNull() & ~F.col("status").isin(["COMPLETED", "RETURNED"]))
    .select("*", F.substring("status", 1, 4).alias("status_prefix"))
)

result = (
    base
    .select("id", "channel", "amount", "shipping_fee", "discount")
    .unpivot(
        ["id", "channel"],
        ["amount", "shipping_fee", "discount"],
        "kpi",
        "reading",
    )
)
