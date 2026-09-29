from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("shipped_date").desc(), "id")
w2 = Window.partitionBy("status").orderBy(F.col("cost").desc(), "id")
w3 = Window.partitionBy("order_id").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy("shipped_date")
amount_by_order_id = df_payments.groupBy("order_id").agg(F.max("amount").alias("max_payment_amount"))

cleaned = (
    df_shipments
    .filter((F.col("shipped_date") >= F.lit("2023-05-29").cast("date")) & (F.col("delivered_date") <= "2025-07-01"))
    .select(
        "*",
        F.datediff(F.current_date(), "shipped_date").alias("days_since_shipped_date"),
    )
)

deduped = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

enriched = (
    deduped
    .select(
        "*",
        F.ntile(10).over(w2).alias("cost_bucket"),
        F.sum("days_since_shipped_date").over(w3).alias("cumulative_days_since_shipped_date"),
    )
)

ranked = (
    enriched
    .withColumn("rn", F.rank().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "cost",
        "days_since_shipped_date",
        "cost_bucket",
    )
)

result = (
    ranked
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "days_since_shipped_date",
        "cost_bucket",
        F.when(F.col("carrier").rlike("^(FedEx|Kerry)$"), "GROUP_B").otherwise("PRIORITY").alias("carrier_zone"),
        F.col("max_payment_amount"),
    )
    .orderBy(F.col("max_payment_amount").desc_nulls_first(), "id")
    .limit(5)
)
