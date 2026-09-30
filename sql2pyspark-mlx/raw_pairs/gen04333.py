from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.year("paid_at"), "method").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("method")
w3 = Window.partitionBy("order_id").orderBy("amount", "id")
w4 = Window.partitionBy("order_id").orderBy("first_known_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("method_max_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("first_known_method_max_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("event_seq", "id")

valid = (
    df_payments
    .filter(F.col("method").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        F.trim("method").alias("clean_method"),
    )
)

enriched = (
    valid
    .select(
        "id",
        "order_id",
        "amount",
        "clean_method",
        F.first("amount", ignorenulls=True).over(w).alias("first_known_amount"),
        F.max("amount").over(w2).alias("method_max_amount"),
    )
)

enriched_payments = (
    enriched
    .select(
        "id",
        "amount",
        "clean_method",
        "first_known_amount",
        F.row_number().over(w3).alias("event_seq"),
        F.sum("first_known_amount").over(w4).alias("cumulative_first_known_amount"),
        F.first("method_max_amount", ignorenulls=True).over(w5).alias("first_known_method_max_amount"),
    )
)

scored = (
    enriched_payments
    .filter(F.col("first_known_method_max_amount") >= 1171)
    .select(
        "id",
        "amount",
        "cumulative_first_known_amount",
        "first_known_method_max_amount",
        F.sum("amount").over(w6).alias("running_amount"),
        F.ntile(4).over(w7).alias("event_seq_bucket"),
    )
)

report = scored.select("id", "amount", "cumulative_first_known_amount")

result = report.select("id", "amount", "cumulative_first_known_amount")
