from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee").isNotNull())
    .select("shipping_fee")
    .subtract(df_shipments.filter(F.col("cost").isNotNull()).select("cost"))
)
