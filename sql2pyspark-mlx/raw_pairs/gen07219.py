from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("signup_date").desc_nulls_first(), "id")
w2 = Window.orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city", "country")
w5 = Window.partitionBy("segment").orderBy("is_active_flag", "id").rowsBetween(-6, -1)
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())
w7 = Window.partitionBy("city").orderBy("order_order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_customers
    .filter(F.col("credit_limit").between(8900, 12700))
    .withColumn("is_active_flag", F.col("is_active").cast("int"))
    .withColumn("city_filled", F.coalesce("city", F.lit("NONE")))
)

latest = (
    filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_windows = (
    latest
    .select(
        "*",
        F.first("city", ignorenulls=True).over(w2).alias("first_known_city"),
        F.last("age", ignorenulls=True).over(w3).alias("last_known_age"),
        F.corr("age", "credit_limit").over(w4).alias("corr_age_credit_limit"),
        F.avg("age").over(w5).alias("age_avg_6_prior"),
    )
)

latest_match = (
    with_windows
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.email",
        "s.city",
        "s.segment",
        "s.is_active",
        "s.credit_limit",
        "s.age",
        "s.last_known_age",
        "s.corr_age_credit_limit",
        "s.age_avg_6_prior",
        F.col("c.amount"),
        F.col("c.order_date").alias("order_order_date"),
    )
)

enriched = (
    latest_match
    .select(
        "*",
        F.last("city", ignorenulls=True).over(w7).alias("last_known_city"),
        F.sum("corr_age_credit_limit").over(w2).alias("running_corr_age_credit_limit"),
    )
)

result = (
    enriched
    .groupBy("segment")
    .pivot("city", ["Berlin", "Singapore", "Tokyo"])
    .agg(F.sum("running_corr_age_credit_limit"))
)
