from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment")
w2 = Window.orderBy("age", "id")
w_ordered = Window.partitionBy("country", "segment").orderBy(F.col("age").desc())
w4 = Window.partitionBy("country", "segment").orderBy("credit_limit", "id")
w5 = Window.partitionBy("city", "country").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("segment", "city").orderBy("signup_date")
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w8 = Window.partitionBy("city").orderBy("shipping_fee", "id").rowsBetween(-6, Window.currentRow)

scored = (
    df_customers
    .filter(F.col("city").isin(["Tokyo", "Chiang Mai", "London"]))
    .select(
        "*",
        F.round((F.col("age") / F.sum("age").over(w)), 4).alias("age_share"),
        F.row_number().over(w2).alias("seq_no"),
        F.cume_dist().over(w_ordered).alias("age_cume_dist"),
        F.lead("age", 1).over(w4).alias("next_age"),
        F.first("age", ignorenulls=True).over(w5).alias("first_known_age"),
    )
)

latest = (
    scored
    .withColumn("latest_rank", F.rank().over(w6))
    .filter(F.col("latest_rank") == 1)
)

with_last = (
    latest
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.name",
        "s.country",
        "s.city",
        "s.credit_limit",
        "s.age",
        "s.seq_no",
        "s.next_age",
        "s.first_known_age",
        F.col("c.order_date").alias("order_order_date"),
        F.col("c.shipping_fee"),
    )
)

scored_customers = (
    with_last
    .select(
        "id",
        "name",
        "age",
        "next_age",
        "shipping_fee",
        F.sum("credit_limit").over(w8).alias("last7_sum_credit_limit"),
        F.min("credit_limit").over(w8).alias("last7_min_credit_limit"),
        F.avg("credit_limit").over(w8).alias("last7_avg_credit_limit"),
        F.max("credit_limit").over(w8).alias("last7_max_credit_limit"),
    )
)

kept = (
    scored_customers
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.to_date(F.col("c.started_at")) < "2023-08-23"), "left_semi")
)

result = (
    kept
    .groupBy(
        F.when(F.col("shipping_fee") >= 31, "LARGE").otherwise("SMALL").alias("shipping_fee_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("shipping_fee", [0.25, 0.5, 0.75]).alias("shipping_fee_quartiles"),
        F.percentile_approx("age", [0.25, 0.5, 0.75]).alias("age_quartiles"),
    )
)
