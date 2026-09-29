from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("method", "order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_payments
    .select(
        "id",
        "method",
        F.max("amount").over(w_part).alias("running_max_amount"),
        F.count("amount").over(w_part).alias("running_count_amount"),
        F.sum("amount").over(w_part).alias("running_sum_amount"),
        F.min("amount").over(w_part).alias("running_min_amount"),
    )
)

result = (
    scored
    .select(
        "id",
        "method",
        "running_count_amount",
        "running_min_amount",
        F.when(F.col("method").rlike("^(cash|card)$"), "OTHER_ZONE").when(F.col("method").rlike("^(paypal|bank_transfer)$"), "CORE").otherwise("PRIORITY").alias("method_zone"),
    )
)
