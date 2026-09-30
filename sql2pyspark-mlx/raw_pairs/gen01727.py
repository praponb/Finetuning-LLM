from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy(F.col("amount").desc_nulls_first(), "id")

base = (
    df_payments
    .filter(F.col("paid_at") > F.to_date(F.lit("2023-10-14")))
    .withColumn("status_group", F.when(F.col("status").isin(["SUCCESS", "REFUNDED"]), "Core").otherwise("Other"))
    .withColumn("method_lower", F.lower("method"))
    .withColumn("resolved_method", F.coalesce("method", F.lit("N/A")))
)

cleaned = (
    base
    .filter(F.col("method").isNotNull())
    .withColumn("amount_whole", F.floor("amount"))
    .withColumn("method_filled", F.coalesce("method", F.lit("N/A")))
)

filtered = (
    cleaned
    .filter(~F.col("method").eqNullSafe(F.lit("bank_transfer")))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status_group",
        "method_lower",
        "amount_whole",
        "method_filled",
        F.date_format("paid_at", "yyyy-MM").alias("paid_at_label"),
        F.dayofmonth("paid_at").alias("paid_at_day"),
        F.coalesce("method", F.lit("UNKNOWN")).alias("resolved_method_2"),
    )
)

latest = (
    filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "order_id", "method", "method_lower", "method_filled")
)

result = (
    latest
    .groupingSets([["method", "order_id"], ["method"]], "method", "order_id")
    .agg(
        F.count_if(F.col("method") == "cash").alias("cash_events"),
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("method") == "card", 1)).alias("card_count"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.grouping("method").alias("gid"),
    )
    .filter(F.col("distinct_order_ids") < 1000)
    .orderBy(F.col("method").desc_nulls_first())
)
