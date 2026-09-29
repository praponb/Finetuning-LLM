from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy(F.col("age").asc_nulls_last())
w2 = Window.partitionBy("segment").orderBy("age", "id")
w3 = Window.partitionBy("segment").orderBy("age", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("segment").orderBy(F.col("age_bucket").desc(), "id")
w5 = Window.partitionBy("segment").orderBy(F.col("age_bucket").desc_nulls_last())
w6 = Window.partitionBy("segment").orderBy("age", "id").rowsBetween(-5, -1)
w7 = Window.partitionBy("segment").orderBy(F.col("age_rank").desc_nulls_last())

base = (
    df_customers
    .filter(F.col("credit_limit") > 6500)
    .select(
        "id",
        "name",
        "email",
        "segment",
        "is_active",
        "age",
        F.coalesce("city", F.lit("N/A")).alias("city_filled"),
    )
)

metrics = (
    base
    .select(
        "id",
        "segment",
        "is_active",
        "age",
        "city_filled",
        F.rank().over(w_recent).alias("age_rank"),
        F.ntile(3).over(w2).alias("age_bucket"),
        F.avg("age").over(w3).alias("age_avg_5"),
    )
)

scored = (
    metrics
    .withColumn("age_bucket_bucket", F.ntile(3).over(w4))
    .withColumn("age_bucket_cume_dist", F.cume_dist().over(w5))
    .withColumn("age_avg_5_prior", F.avg("age").over(w6))
)

ranked = (
    scored
    .withColumn("rn", F.rank().over(w7))
    .filter(F.col("rn") == 1)
)

eligible = (
    ranked
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.shipping_fee") > 40), "left_anti")
)

result = (
    eligible
    .select(
        "id",
        "segment",
        "is_active",
        "age",
        "city_filled",
        "age_rank",
        "age_bucket",
        "age_avg_5",
        "age_bucket_bucket",
    )
)
