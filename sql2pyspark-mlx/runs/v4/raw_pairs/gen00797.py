from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("method").orderBy("is_paypal", "id")

valid = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2024-04-20").cast("date")) & F.col("method").isNotNull() & F.col("method").isin(["paypal", "cash", "promptpay"]))
    .select(
        "id",
        "order_id",
        "method",
        "paid_at",
        F.when(F.col("status") == "SUCCESS", 1).otherwise(0).alias("is_success"),
        F.when(F.col("method") == "paypal", 1).otherwise(0).alias("is_paypal"),
    )
)

result = (
    valid
    .select(
        "*",
        F.sum("is_success").over(w).alias("running_is_success"),
        F.ntile(10).over(w2).alias("is_paypal_bucket"),
    )
)
