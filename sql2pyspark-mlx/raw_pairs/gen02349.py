from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("carrier", "order_id").orderBy("shipped_date_minus_19d", "id").rowsBetween(-6, Window.currentRow)
w_recent = Window.partitionBy("status").orderBy("last7_avg_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_shipments
    .filter((F.col("delivered_date") >= F.lit("2023-06-14").cast("date")) & F.col("carrier").isNotNull())
    .withColumn("shipped_date_minus_19d", F.date_sub("shipped_date", 19))
)

metrics = (
    valid
    .select(
        "*",
        F.max("cost").over(w_part).alias("last7_max_cost"),
        F.count("cost").over(w_part).alias("last7_count_cost"),
        F.sum("cost").over(w_part).alias("last7_sum_cost"),
        F.avg("cost").over(w_part).alias("last7_avg_cost"),
    )
)

valid_shipments = (
    metrics
    .filter(F.col("delivered_date") > F.to_date(F.lit("2023-07-11")))
    .withColumn("last7_count_cost_tier", F.when(F.col("last7_count_cost") >= 15, "High").when(F.col("last7_count_cost") >= 4, "Medium").otherwise("Low"))
)

scored = (
    valid_shipments
    .withColumn("running_sum_last7_avg_cost", F.sum("last7_avg_cost").over(w_recent))
    .withColumn("running_count_last7_avg_cost", F.count("last7_avg_cost").over(w_recent))
    .withColumn("running_avg_last7_avg_cost", F.avg("last7_avg_cost").over(w_recent))
)

kept_keys = (
    scored
    .select("status", "carrier")
    .subtract(
        df_shipments
        .filter(F.col("cost") <= 39)
        .select("status", "carrier")
    )
)

verified = (
    scored
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.carrier") == F.col("k.carrier")), "inner")
    .select("s.*")
)

joined = (
    verified
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id").eqNullSafe(F.col("d.id")), "left")
    .select("a.*", F.col("d.shipping_fee"), F.col("d.channel"), F.col("d.amount"))
)

result = (
    joined
    .select("channel", F.col("cost").alias("COST"), "last7_avg_cost", "last7_sum_cost")
    .unpivot(
        ids=["channel"],
        values=["COST", "last7_avg_cost", "last7_sum_cost"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
