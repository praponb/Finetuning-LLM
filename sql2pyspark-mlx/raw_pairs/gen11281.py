from pyspark.sql import functions as F

amount_by_order_id = df_payments.groupBy("order_id").agg(F.max("amount").alias("max_payment_amount"))

kept_keys = (
    df_order_items
    .select("product_id")
    .subtract(
        df_order_items
        .filter(F.col("quantity") != 4)
        .select("product_id")
    )
)

kept_rows = (
    df_order_items
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .join(amount_by_order_id, on="order_id", how="left")
    .select("id", "order_id", "quantity", "unit_price", F.col("max_payment_amount"))
    .orderBy(F.col("unit_price").desc_nulls_first())
)
