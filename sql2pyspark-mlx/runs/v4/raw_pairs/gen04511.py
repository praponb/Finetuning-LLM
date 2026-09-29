from pyspark.sql import functions as F

result = (
    df_orders
    .select("id", "customer_id", "shipping_fee", "amount", "discount")
    .unpivot(
        ["id", "customer_id"],
        ["shipping_fee", "amount", "discount"],
        "metric_name",
        "reading",
    )
    .filter(F.col("reading").isNotNull())
)
