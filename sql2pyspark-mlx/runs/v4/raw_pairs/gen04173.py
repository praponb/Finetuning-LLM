from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("credit_limit").asc_nulls_last())
w_grp = Window.partitionBy("city").orderBy(F.col("age").desc())
w3 = Window.partitionBy("country").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("segment")
w_ordered = Window.partitionBy("city").orderBy("age", "id").rowsBetween(-5, Window.currentRow)
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.order_date").desc(), F.col("c.id").desc())

base = (
    df_customers
    .filter((F.col("signup_date") >= F.to_date(F.lit("2023-07-06"))) & (F.col("signup_date") <= "2025-10-23"))
    .select(
        "*",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
    )
)

ranked = (
    base
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

enriched = (
    ranked
    .withColumn("age_percent_rank", F.percent_rank().over(w_grp))
    .withColumn("running_credit_limit", F.sum("credit_limit").over(w3))
    .withColumn("segment_sum_credit_limit", F.sum("credit_limit").over(w4))
    .withColumn("age_avg_5", F.avg("age").over(w_ordered))
)

latest_match = (
    enriched
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.discount").alias("order_discount"))
)

result = (
    latest_match
    .select(
        "id",
        "name",
        "email",
        "country",
        "signup_date",
        "is_active",
        "credit_limit",
        "is_active_label",
        "running_credit_limit",
    )
)
