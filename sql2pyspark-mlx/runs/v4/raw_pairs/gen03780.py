from pyspark.sql import functions as F

result = (
    df_orders
    .select("id", "customer_id", "amount", "shipping_fee", "discount")
    .unpivot(
        ["id", "customer_id"],
        ["amount", "shipping_fee", "discount"],
        "kpi",
        "reading",
    )
    .filter(F.col("reading").isNotNull())
)
