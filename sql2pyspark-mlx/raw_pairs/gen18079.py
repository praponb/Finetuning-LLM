from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("status")
w2 = Window.partitionBy("customer_id").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_orders
    .filter(F.col("channel").isin(["partner", "web", "store"]))
    .select(
        "id",
        "customer_id",
        "amount",
        "shipping_fee",
        F.corr("amount", "discount").over(w_hist).alias("corr_amount_discount"),
        F.sum("amount").over(w2).alias("cumulative_amount"),
    )
)

result = (
    scored
    .select("id", "customer_id", "amount", "corr_amount_discount")
    .orderBy("customer_id")
)
