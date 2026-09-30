from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w2 = Window.partitionBy("segment")
w3 = Window.partitionBy("k")

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("k", "v"),
    )
)

kept_keys = (
    exploded
    .select("id", "customer_id")
    .subtract(
        df_carts
        .filter(F.col("customer_id").isNotNull())
        .select("id", "customer_id")
    )
)

reconciled = (
    exploded
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

with_dims = (
    reconciled
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .select("a.*", F.col("c.age"), F.col("c.segment"))
)

metrics = (
    with_dims
    .withColumn("age_sum_2h", F.sum("age").over(w))
    .withColumn("segment_min_age", F.min("age").over(w2))
    .withColumn("k_avg_age", F.avg("age").over(w3))
)

result = (
    metrics
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "k",
        "v",
        "segment",
        "segment_min_age",
        "k_avg_age",
        F.when(F.col("segment").rlike("^(SMB|Corporate)$"), "CORE").when(F.col("segment").rlike("^(Consumer)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("segment_zone"),
    )
    .orderBy(F.col("segment_zone").asc_nulls_first())
)
