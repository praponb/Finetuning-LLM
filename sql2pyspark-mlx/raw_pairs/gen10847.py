from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("paid_at_quarter").asc_nulls_last())
w2 = Window.partitionBy("order_id").orderBy(F.col("paid_at_quarter").desc_nulls_last())
w3 = Window.partitionBy("order_id", "method").orderBy(F.col("paid_at_quarter_percent_rank").asc_nulls_first(), "id")

cleaned = (
    df_payments
    .filter(F.col("method").isNotNull() & F.col("order_id").isin([8, 20, 26]))
    .withColumn("paid_at_quarter", F.quarter("paid_at"))
    .withColumn("method_filled", F.coalesce("method", F.lit("Unknown")))
    .withColumn("resolved_method", F.coalesce("method", F.lit("OTHER")))
)

valid = (
    cleaned
    .filter((~F.col("method").eqNullSafe(F.lit("paypal"))) & F.col("order_id").isNotNull())
    .withColumn("paid_at_label", F.date_format("paid_at", "yyyy-MM"))
    .withColumn("method_group", F.when(F.col("method").isin(["card", "paypal"]), "Priority").otherwise("Other"))
    .withColumn("method_filled_2", F.coalesce("method", F.lit("DIRECT")))
)

metrics = (
    valid
    .withColumn("paid_at_quarter_rank", F.rank().over(w))
    .withColumn("paid_at_quarter_percent_rank", F.percent_rank().over(w2))
)

deduped = (
    metrics
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "method",
        "amount",
        "status",
        "paid_at_quarter",
        "paid_at_label",
        "method_filled_2",
        "paid_at_quarter_rank",
        "paid_at_quarter_percent_rank",
    )
)

result = (
    deduped
    .select("id", "status", "paid_at_quarter_percent_rank", "amount")
    .unpivot(
        ids=["id", "status"],
        values=["paid_at_quarter_percent_rank", "amount"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
