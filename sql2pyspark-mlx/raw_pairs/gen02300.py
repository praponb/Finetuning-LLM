from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy(F.month("order_date"), "product_id").orderBy(F.col("unit_price").desc_nulls_first())
w2 = Window.partitionBy("product_id").orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy(F.col("unit_price").asc_nulls_first(), "id")
amount_by_order_id = df_payments.groupBy("order_id").agg(F.max("amount").alias("max_payment_amount"))

joined = (
    df_order_items
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "left")
    .select(
        "a.id",
        "a.order_id",
        "a.product_id",
        "a.quantity",
        "a.unit_price",
        F.col("d.order_date"),
        F.col("d.amount"),
        F.col("d.discount"),
    )
)

scored = (
    joined
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        "order_date",
        F.rank().over(w_ordered).alias("unit_price_rank"),
        F.sum("quantity").over(w2).alias("cumulative_quantity"),
    )
)

enriched = (
    scored
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "inner")
    .select(
        "src.id",
        "src.order_id",
        "src.unit_price",
        "src.order_date",
        "src.unit_price_rank",
        F.col("b.order_date").alias("order_order_date"),
        F.col("b.discount"),
    )
)

joined_order_items = (
    enriched
    .alias("a")
    .join(df_orders.alias("b"), F.col("a.order_id") == F.col("b.id"), "left")
    .filter(F.col("b.discount") > 0.2)
    .select("a.*", F.col("b.order_date").alias("order_order_date_2"), F.col("b.status"))
)

deduped = (
    joined_order_items
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

stats = (
    deduped
    .withColumn("quarter_start", F.date_trunc("QUARTER", "order_order_date"))
    .groupingSets(
        [["quarter_start", "order_id"], ["quarter_start"], []],
        "quarter_start",
        "order_id",
    )
    .agg(
        F.count(F.when(F.col("status") == "PENDING", 1)).alias("pending_count"),
        F.count_if(F.col("status") == "SHIPPED").alias("shipped_events"),
        F.grouping_id().alias("gid"),
    )
)

result = (
    stats
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "quarter_start",
        "order_id",
        "pending_count",
        "shipped_events",
        F.col("max_payment_amount"),
    )
)
