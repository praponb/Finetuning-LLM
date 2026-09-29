from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("element").orderBy(F.col("is_welcome5").asc_nulls_last())
w3 = Window.partitionBy("element").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w4 = Window.partitionBy("country").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w5 = Window.partitionBy("element")

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode("coupon_codes").alias("element"),
    )
)

kept_keys = (
    lines
    .select("customer_id")
    .intersect(
        df_carts
        .filter(F.hour("created_at").between(6, 14))
        .select("customer_id")
    )
)

kept_rows = (
    lines
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

cleaned = (
    kept_rows
    .filter((F.col("created_at") >= F.to_timestamp(F.lit("2025-03-02 09:00:00"))) & F.col("element").isin(["VIP", "WELCOME5", "SPRING10"]))
    .select(
        "*",
        F.when(F.col("element") == "WELCOME5", 1).otherwise(0).alias("is_welcome5"),
    )
)

with_dims = (
    cleaned
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.country"), F.col("p.email"))
)

with_windows = (
    with_dims
    .withColumn("customer_id_avg_is_welcome5", F.avg("is_welcome5").over(w))
    .withColumn("is_welcome5_dense_rank", F.dense_rank().over(w2))
    .withColumn("is_welcome5_avg_15m", F.avg("is_welcome5").over(w3))
    .withColumn("is_welcome5_avg_30m", F.avg("is_welcome5").over(w4))
    .withColumn("element_max_is_welcome5", F.max("is_welcome5").over(w5))
)

result = (
    with_windows
    .select(
        "customer_id",
        "is_welcome5",
        "country",
        "email",
        "is_welcome5_avg_15m",
        "element_max_is_welcome5",
    )
    .orderBy(F.col("country").desc_nulls_last(), "is_welcome5")
)
