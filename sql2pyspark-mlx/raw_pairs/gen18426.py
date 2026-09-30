from pyspark.sql import functions as F

max_discount = df_orders.filter(F.col("status").isin(["CANCELLED", "RETURNED"])).agg(F.max("discount")).first()[0]

result = (
    df_orders
    .filter(F.col("discount") > max_discount)
    .select("id", "discount", "channel", "shipping_fee")
)
