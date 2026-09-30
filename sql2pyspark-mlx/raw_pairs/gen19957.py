from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("signup_date").desc_nulls_first(), "id")
w2 = Window.partitionBy("city").orderBy(F.col("signup_date").desc_nulls_first(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.order_date").desc(), F.col("c.id").desc())

deduped = (
    df_customers
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

eligible = (
    deduped
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.order_date") < F.col("s.signup_date")), "left_anti")
)

deduped_customers = (
    eligible
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "name", "email", "signup_date", "is_active", "latest_rank")
)

with_last = (
    deduped_customers
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.name", "s.is_active", F.col("c.discount"))
)

result = (
    with_last
    .groupBy(
        F.when(F.col("discount") >= 0.32, "LARGE").otherwise("SMALL").alias("discount_tier"),
    )
    .agg(
        F.percentile_approx("discount", [0.25, 0.5, 0.75]).alias("discount_quartiles"),
        F.sum("discount").alias("total_discount"),
        F.percentile_approx("discount", [0.25, 0.5, 0.75]).alias("discount_quartiles_2"),
        F.percentile_approx("discount", 0.95).alias("discount_p95"),
        F.avg("discount").alias("avg_discount"),
    )
    .orderBy(F.col("discount_p95").asc_nulls_first())
)
