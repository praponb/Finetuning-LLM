from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("line_total").desc_nulls_first(), "id")
amount_by_order_id = df_payments.groupBy("order_id").agg(F.sum("amount").alias("sum_payment_amount"))

cleaned = (
    df_order_items
    .filter(F.col("order_id") == 28)
    .select("*", (F.col("quantity") - F.col("unit_price")).alias("line_total"))
)

latest = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

cleaned_order_items = (
    latest
    .filter(F.col("unit_price").isNull())
    .select(
        "id",
        "order_id",
        "unit_price",
        "line_total",
        "rn",
        (F.col("line_total") - F.col("quantity")).alias("line_total_minus_quantity"),
    )
)

remaining_keys = (
    cleaned_order_items
    .select("id", "order_id")
    .subtract(
        df_order_items
        .filter(F.col("order_id").isin([7, 8, 19]))
        .select("id", "order_id")
    )
)

reconciled = (
    cleaned_order_items
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "order_id",
        "unit_price",
        "rn",
        "line_total_minus_quantity",
        F.col("sum_payment_amount"),
    )
    .orderBy(F.col("line_total_minus_quantity").desc())
)
