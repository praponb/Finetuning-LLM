from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("status", "plan").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("months_start_date_to_end_date", "id")
w3 = Window.partitionBy("customer_id", "status").orderBy(F.col("start_date").desc_nulls_last(), "id")
shipping_fee_by_customer_id = df_orders.groupBy("customer_id").agg(F.sum("shipping_fee").alias("sum_order_shipping_fee"))

filtered = (
    df_subscriptions
    .filter((F.col("end_date") >= F.lit("2023-06-06").cast("date")) & F.col("customer_id").isin([3, 8]))
    .select(
        "*",
        F.length("plan").alias("plan_length"),
        F.round((F.datediff("end_date", "start_date") / 30), 1).alias("months_start_date_to_end_date"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.avg("plan_length").over(w_part).alias("running_avg_plan_length"),
        F.sum("plan_length").over(w_part).alias("running_sum_plan_length"),
        F.max("plan_length").over(w_part).alias("running_max_plan_length"),
        F.lead("months_start_date_to_end_date", 1, 0).over(w2).alias("next_months_start_date_to_end_date"),
    )
)

deduped = (
    metrics
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

with_dims = (
    deduped
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id").eqNullSafe(F.col("c.id")), "inner")
    .filter(F.col("c.credit_limit") >= 13900)
    .select("e.*", F.col("c.country"), F.col("c.credit_limit"))
)

result = (
    with_dims
    .join(shipping_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "start_date",
        "monthly_fee",
        "status",
        "months_start_date_to_end_date",
        "running_avg_plan_length",
        "running_sum_plan_length",
        "next_months_start_date_to_end_date",
        F.col("sum_order_shipping_fee"),
    )
    .orderBy(F.col("months_start_date_to_end_date").desc())
)
